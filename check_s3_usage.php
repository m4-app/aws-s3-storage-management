#!/usr/bin/env php
<?php
/** 
 * check_s3_usage.php
 *
 * Lê clientes do RDS (tabela medic_clinica), monta prefixo S3 "uploads/{base64(codigo_acesso)}/",
 * lista objetos no S3 para cada cliente e soma tamanhos, grava resultados em
 * medic_global.global_medic_clinica_storage, com estimativa de custo por chamadas ListObjectsV2.
 *
 * Requisitos:
 *  - PHP 8.x, Composer, aws/aws-sdk-php
 *  - Credenciais AWS em ~/.aws/credentials (perfil default) e região em ~/.aws/config ou passado aqui
 *
 * Estimativa de custo:
 *  - ListObjectsV2 custa ~ US$ 0.005 por 1.000 requisições.
 *  - Cada chamada retorna até 1000 objetos.
 *  - Fórmula usada: cost = api_calls * (0.005 / 1000.0)
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\Exception\AwsException;
use Ramsey\Uuid\Uuid;

// ===================== CONFIG E CONSTANTES =====================
const LIST_PRICE_PER_1000 = 0.005;         // USD por 1000 ListObjectsV2
const PAGE_SIZE           = 1000;          // S3 máximo por página
const LOG_FILE            = '/var/log/s3-size-check.log';

// ===================== PARSE DE ARGUMENTOS =====================
$options = getopt('', [
    'bucket:',
    'region:',
    'base-prefix:',
    'db-host:',
    'db-name:',
    'db-schema-out:',
    'table-out:',
    'statuses:',
    'include-not-in::',      // default false
    'compute-total-bucket::' // default false
]);

function mustGet(array $opts, string $key): string {
    if (!isset($opts[$key]) || trim((string)$opts[$key]) === '') {
        fwrite(STDERR, "ERRO: parâmetro obrigatório --{$key} ausente.\n");
        exit(1);
    }
    return (string)$opts[$key];
}

$bucket       = mustGet($options, 'bucket');
$region       = mustGet($options, 'region');
$basePrefix   = mustGet($options, 'base-prefix');   // tipicamente "uploads/"
$dbHost       = mustGet($options, 'db-host');
$dbName       = mustGet($options, 'db-name');       // medicdb
$dbSchemaOut  = mustGet($options, 'db-schema-out'); // medic_global
$tableOut     = mustGet($options, 'table-out');     // global_medic_clinica_storage
$statusesStr  = mustGet($options, 'statuses');      // exemplo: "Y,B,S,G"

$includeNotIn = isset($options['include-not-in']) ? filter_var($options['include-not-in'], FILTER_VALIDATE_BOOLEAN) : false;
$computeTotalBucket = isset($options['compute-total-bucket']) ? filter_var($options['compute-total-bucket'], FILTER_VALIDATE_BOOLEAN) : false;

$statuses = array_values(array_filter(array_map('trim', explode(',', $statusesStr)), fn($s) => $s !== ''));

// ===================== UTILITÁRIOS =====================
function logMsg(string $msg): void {
    $line = '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL;
    // terminal
    echo $msg . PHP_EOL;
    // arquivo
    @file_put_contents(LOG_FILE, $line, FILE_APPEND);
}

function bytesToGB(int|float $bytes): float {
    // 1 GB = 1,073,741,824 bytes (GiB). Aqui manteremos GB (decimal) = 1,000,000,000 por simplicidade de leitura.
    // Se preferir "GiB", mude o divisor para 1024**3.
    $divisor = 1000 * 1000 * 1000;
    return round(((float)$bytes) / $divisor, 4);
}

function estimateCostUsd(int $apiCalls): float {
    // custo = chamadas * (0.005 / 1000)
    $cost = $apiCalls * (LIST_PRICE_PER_1000 / 1000.0);
    return round($cost, 6);
}

function base64ClientPrefix(int $codigo): string {
    // Ex.: 123456 -> "MTIzNDU2"
    return base64_encode((string)$codigo);
}

function askDbCredentialsInteractively(): array {
    fwrite(STDOUT, "Usuário MySQL (RDS) : ");
    $user = trim(fgets(STDIN));
    // senha sem exibir
    if (strncasecmp(PHP_OS, 'WIN', 3) === 0) {
        // Windows não tem shell - mantemos simples
        fwrite(STDOUT, "Senha MySQL (RDS)  : ");
        $pass = trim(fgets(STDIN));
    } else {
        // Linux/macOS: tentar modo silencioso
        $pass = shell_exec('bash -c \'read -s -p "Senha MySQL (RDS)  : " pwd; echo $pwd\'');
        $pass = trim((string)$pass);
        fwrite(STDOUT, PHP_EOL);
    }
    return [$user, $pass];
}

// ===================== CONEXÕES =====================
function connectDatabase(string $host, string $db, string $user, string $pass): PDO {
    $dsn = "mysql:host={$host};dbname={$db};charset=utf8mb4";
    $opts = [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
    ];
    return new PDO($dsn, $user, $pass, $opts);
}

function createS3Client(string $region): S3Client {
    // Credenciais via perfil default (~/.aws/credentials). Se quiser perfil específico, ajuste aqui.
    return new S3Client([
        'version' => 'latest',
        'region'  => $region,
    ]);
}

// ===================== S3: LISTAGEM E SOMA =====================
/**
 * Lista todos os objetos de um prefixo e soma tamanhos.
 * Retorna [bytes_total, objects_count, api_calls]
 */
function sumPrefixSize(S3Client $s3, string $bucket, ?string $prefix = null): array {
    $bytesTotal   = 0;
    $objectsCount = 0;
    $apiCalls     = 0;

    $params = [
        'Bucket' => $bucket,
        'MaxKeys' => PAGE_SIZE,
    ];
    if ($prefix !== null) {
        $params['Prefix'] = $prefix;
    }

    try {
        do {
            $apiCalls++;
            $result = $s3->listObjectsV2($params);

            if (isset($result['Contents'])) {
                foreach ($result['Contents'] as $obj) {
                    $bytesTotal   += (int)$obj['Size'];
                    $objectsCount += 1;
                }
            }

            if ($result['IsTruncated'] ?? false) {
                $params['ContinuationToken'] = $result['NextContinuationToken'];
            } else {
                $params['ContinuationToken'] = null;
            }
        } while (!empty($params['ContinuationToken']));
    } catch (AwsException $e) {
        logMsg("ERRO S3 listando prefixo [" . ($prefix ?? '(bucket inteiro)') . "]: " . $e->getAwsErrorMessage());
        throw $e;
    }

    return [
        'bytes_total'   => $bytesTotal,
        'objects_count' => $objectsCount,
        'api_calls'     => $apiCalls
    ];
}

// ===================== DB: BUSCAS DE CLIENTES =====================
function getClientsByStatuses(PDO $pdo, array $statuses): iterable {
    // Filtra somente clientes com codigo_acesso 6 dígitos (100000..999999)
    $in = implode(',', array_fill(0, count($statuses), '?'));
    $sql = "SELECT codigo_acesso, status
              FROM medic_clinica
             WHERE status IN ($in)
               AND codigo_acesso BETWEEN 100000 AND 999999
               AND codigo_acesso IS NOT NULL
               AND codigo_acesso <> 0";

    $stmt = $pdo->prepare($sql);
    foreach ($statuses as $i => $st) {
        $stmt->bindValue($i+1, $st);
    }
    $stmt->execute();

    while ($row = $stmt->fetch()) {
        yield ['codigo_acesso' => (int)$row['codigo_acesso'], 'status' => $row['status']];
    }
    $stmt->closeCursor();
}

function getClientsNotInStatuses(PDO $pdo, array $statuses): iterable {
    $in = implode(',', array_fill(0, count($statuses), '?'));
    $sql = "SELECT codigo_acesso, status
              FROM medic_clinica
             WHERE status NOT IN ($in)
               AND codigo_acesso BETWEEN 100000 AND 999999
               AND codigo_acesso IS NOT NULL
               AND codigo_acesso <> 0";

    $stmt = $pdo->prepare($sql);
    foreach ($statuses as $i => $st) {
        $stmt->bindValue($i+1, $st);
    }
    $stmt->execute();

    while ($row = $stmt->fetch()) {
        yield ['codigo_acesso' => (int)$row['codigo_acesso'], 'status' => $row['status']];
    }
    $stmt->closeCursor();
}

// ===================== DB: INSERÇÃO DE RESULTADOS =====================
function insertResultRow(
    PDO $pdo, string $schema, string $table, array $data
): void {
    $sql = "INSERT INTO `{$schema}`.`{$table}` 
           (run_id, computed_at, scope, bucket, base_prefix, codigo_acesso, status, prefix_base64,
            objects_count, bytes_total, gb_total, api_calls, cost_estimated_usd, statuses_filter, extra_note)
     VALUES(:run_id, :computed_at, :scope, :bucket, :base_prefix, :codigo_acesso, :status, :prefix_base64,
            :objects_count, :bytes_total, :gb_total, :api_calls, :cost_estimated_usd, :statuses_filter, :extra_note)";

    $stmt = $pdo->prepare($sql);
    $stmt->execute([
        ':run_id'            => $data['run_id'],
        ':computed_at'       => $data['computed_at'],
        ':scope'             => $data['scope'],
        ':bucket'            => $data['bucket'],
        ':base_prefix'       => $data['base_prefix'],
        ':codigo_acesso'     => $data['codigo_acesso'],
        ':status'            => $data['status'],
        ':prefix_base64'     => $data['prefix_base64'],
        ':objects_count'     => $data['objects_count'],
        ':bytes_total'       => $data['bytes_total'],
        ':gb_total'          => $data['gb_total'],
        ':api_calls'         => $data['api_calls'],
        ':cost_estimated_usd'=> $data['cost_estimated_usd'],
        ':statuses_filter'   => $data['statuses_filter'],
        ':extra_note'        => $data['extra_note'],
    ]);
}

// ===================== FUNÇÕES DE ALTO NÍVEL =====================
/**
 * Tamanho total do bucket (sem prefixo) — CUIDADO: pode ser caro/lento.
 * Retorna custo estimado desta operação.
 */
function getTotalBucketSize(S3Client $s3, string $bucket): array {
    logMsg("Iniciando soma do BUCKET INTEIRO: {$bucket} (isso pode demorar/custar mais).");
    $res = sumPrefixSize($s3, $bucket, null);
    $gb  = bytesToGB($res['bytes_total']);
    $cost= estimateCostUsd($res['api_calls']);
    logMsg(sprintf("Total bucket = %.2f GB | Objetos=%d | Calls=%d | Custo Estimado=$%.6f",
        $gb, $res['objects_count'], $res['api_calls'], $cost
    ));
    return [
        'gb_total' => $gb,
        'bytes_total' => $res['bytes_total'],
        'objects_count' => $res['objects_count'],
        'api_calls' => $res['api_calls'],
        'cost_estimated_usd' => $cost
    ];
}

/**
 * Tamanho total por cliente (prefixo uploads/{base64(codigo)}/)
 * Retorna custo estimado desta operação.
 */
function getClientStorageSize(S3Client $s3, string $bucket, string $basePrefix, int $codigo): array {
    $b64 = base64ClientPrefix($codigo);
    $prefix = rtrim($basePrefix, '/') . '/' . $b64 . '/';

    $res = sumPrefixSize($s3, $bucket, $prefix);
    $gb  = bytesToGB($res['bytes_total']);
    $cost= estimateCostUsd($res['api_calls']);

    logMsg(sprintf("Cliente %d (%s): %.2f GB | Objetos=%d | Calls=%d | Custo=$%.6f",
        $codigo, $b64, $gb, $res['objects_count'], $res['api_calls'], $cost
    ));

    return [
        'prefix_base64' => $b64,
        'gb_total' => $gb,
        'bytes_total' => $res['bytes_total'],
        'objects_count' => $res['objects_count'],
        'api_calls' => $res['api_calls'],
        'cost_estimated_usd' => $cost
    ];
}

/**
 * Soma total para clientes cujo status ∈ lista dada.
 * Retorna custo estimado desta operação (soma das chamadas de todos os clientes).
 */
function getTotalByStatus(PDO $pdo, S3Client $s3, string $bucket, string $basePrefix, array $statuses,
                          string $runId, string $schemaOut, string $tableOut, string $statusesFilter): array {
    logMsg("Listando clientes com status IN (" . implode(',', $statuses) . ")…");

    $totalBytes = 0;
    $totalObjects = 0;
    $totalApiCalls = 0;
    $processed = 0;

    foreach (getClientsByStatuses($pdo, $statuses) as $cli) {
        $codigo = (int)$cli['codigo_acesso'];
        $status = (string)$cli['status'];

        $r = getClientStorageSize($s3, $bucket, $basePrefix, $codigo);

        $totalBytes   += $r['bytes_total'];
        $totalObjects += $r['objects_count'];
        $totalApiCalls+= $r['api_calls'];
        $processed++;

        // Grava linha CLIENT
        insertResultRow($pdo, $schemaOut, $tableOut, [
            'run_id'             => $runId,
            'computed_at'        => date('Y-m-d H:i:s'),
            'scope'              => 'CLIENT',
            'bucket'             => $bucket,
            'base_prefix'        => $basePrefix,
            'codigo_acesso'      => $codigo,
            'status'             => $status,
            'prefix_base64'      => $r['prefix_base64'],
            'objects_count'      => $r['objects_count'],
            'bytes_total'        => (string)$r['bytes_total'],
            'gb_total'           => $r['gb_total'],
            'api_calls'          => $r['api_calls'],
            'cost_estimated_usd' => $r['cost_estimated_usd'],
            'statuses_filter'    => $statusesFilter,
            'extra_note'         => null,
        ]);
    }

    $totalGB = bytesToGB($totalBytes);
    $cost = estimateCostUsd($totalApiCalls);
    logMsg(sprintf("SUM IN(%s): %.2f GB | Clientes=%d | Objetos=%d | Calls=%d | Custo=$%.6f",
        implode(',', $statuses), $totalGB, $processed, $totalObjects, $totalApiCalls, $cost
    ));

    // Grava linha SUMMARY
    /*insertResultRow($pdo, $schemaOut, $tableOut, [
        'run_id'             => $runId,
        'computed_at'        => date('Y-m-d H:i:s'),
        'scope'              => 'SUMMARY',
        'bucket'             => $bucket,
        'base_prefix'        => $basePrefix,
        'codigo_acesso'      => null,
        'status'             => null,
        'prefix_base64'      => null,
        'objects_count'      => $totalObjects,
        'bytes_total'        => (string)$totalBytes,
        'gb_total'           => $totalGB,
        'api_calls'          => $totalApiCalls,
        'cost_estimated_usd' => $cost,
        'statuses_filter'    => $statusesFilter,
        'extra_note'         => 'SUMMARY:IN',
    ]);*/

    return [
        'processed_clients'  => $processed,
        'bytes_total'        => $totalBytes,
        'gb_total'           => $totalGB,
        'objects_count'      => $totalObjects,
        'api_calls'          => $totalApiCalls,
        'cost_estimated_usd' => $cost,
    ];
}

/**
 * Soma total para clientes cujo status ∉ lista dada.
 */
function getTotalByStatusNot(PDO $pdo, S3Client $s3, string $bucket, string $basePrefix, array $statuses,
                             string $runId, string $schemaOut, string $tableOut, string $statusesFilter): array {
    logMsg("Listando clientes com status NOT IN (" . implode(',', $statuses) . ")…");

    $totalBytes = 0;
    $totalObjects = 0;
    $totalApiCalls = 0;
    $processed = 0;

    foreach (getClientsNotInStatuses($pdo, $statuses) as $cli) {
        $codigo = (int)$cli['codigo_acesso'];
        $status = (string)$cli['status'];

        $r = getClientStorageSize($s3, $bucket, $basePrefix, $codigo);

        $totalBytes   += $r['bytes_total'];
        $totalObjects += $r['objects_count'];
        $totalApiCalls+= $r['api_calls'];
        $processed++;

        // Grava linha CLIENT
        insertResultRow($pdo, $schemaOut, $tableOut, [
            'run_id'             => $runId,
            'computed_at'        => date('Y-m-d H:i:s'),
            'scope'              => 'CLIENT',
            'bucket'             => $bucket,
            'base_prefix'        => $basePrefix,
            'codigo_acesso'      => $codigo,
            'status'             => $status,
            'prefix_base64'      => base64ClientPrefix($codigo),
            'objects_count'      => $r['objects_count'],
            'bytes_total'        => (string)$r['bytes_total'],
            'gb_total'           => $r['gb_total'],
            'api_calls'          => $r['api_calls'],
            'cost_estimated_usd' => $r['cost_estimated_usd'],
            'statuses_filter'    => $statusesFilter,
            'extra_note'         => null,
        ]);
    }

    $totalGB = bytesToGB($totalBytes);
    $cost = estimateCostUsd($totalApiCalls);
    logMsg(sprintf("SUM NOT IN(%s): %.2f GB | Clientes=%d | Objetos=%d | Calls=%d | Custo=$%.6f",
        implode(',', $statuses), $totalGB, $processed, $totalObjects, $totalApiCalls, $cost
    ));

    // Grava linha SUMMARY
    insertResultRow($pdo, $schemaOut, $tableOut, [
        'run_id'             => $runId,
        'computed_at'        => date('Y-m-d H:i:s'),
        'scope'              => 'SUMMARY',
        'bucket'             => $bucket,
        'base_prefix'        => $basePrefix,
        'codigo_acesso'      => null,
        'status'             => null,
        'prefix_base64'      => null,
        'objects_count'      => $totalObjects,
        'bytes_total'        => (string)$totalBytes,
        'gb_total'           => $totalGB,
        'api_calls'          => $totalApiCalls,
        'cost_estimated_usd' => $cost,
        'statuses_filter'    => $statusesFilter,
        'extra_note'         => 'SUMMARY:NOT_IN',
    ]);

    return [
        'processed_clients'  => $processed,
        'bytes_total'        => $totalBytes,
        'gb_total'           => $totalGB,
        'objects_count'      => $totalObjects,
        'api_calls'          => $totalApiCalls,
        'cost_estimated_usd' => $cost,
    ];
}

// ===================== MAIN =====================
function main(array $opts): void {
    [$dbUser, $dbPass] = askDbCredentialsInteractively();

    $bucket      = $opts['bucket'];
    $region      = $opts['region'];
    $basePrefix  = $opts['base_prefix'];
    $dbHost      = $opts['db_host'];
    $dbName      = $opts['db_name'];
    $schemaOut   = $opts['db_schema_out'];
    $tableOut    = $opts['table_out'];
    $statuses    = $opts['statuses'];
    $statusesFilter = implode(',', $statuses);
    $includeNotIn   = $opts['include_not_in'];
    $computeTotal   = $opts['compute_total_bucket'];

    // Segurança básica do log
    if (!file_exists(LOG_FILE)) {
        @touch(LOG_FILE);
        @chmod(LOG_FILE, 0640);
    }

    logMsg("==== Início da execução ====");
    logMsg("Bucket: {$bucket} | Região: {$region} | Prefixo base: {$basePrefix}");
    logMsg("DB Host: {$dbHost} | DB Name: {$dbName} | Saída: {$schemaOut}.{$tableOut}");
    logMsg("Statuses IN: {$statusesFilter} | Include NOT IN: " . ($includeNotIn ? 'true' : 'false'));
    logMsg("Compute TOTAL BUCKET: " . ($computeTotal ? 'true' : 'false'));

    // Conexões
    $pdo = connectDatabase($dbHost, $dbName, $dbUser, $dbPass);
    $s3  = createS3Client($region);
    $runId = class_exists(Uuid::class) ? Uuid::uuid4()->toString() : bin2hex(random_bytes(16));

    // Caso a lib ramsey/uuid não exista, não tem problema; o fallback usa random_bytes
    // Dica: composer require ramsey/uuid (opcional)

    // 1) SOMA IN(statuses)
    $in = getTotalByStatus($pdo, $s3, $bucket, $basePrefix, $statuses, $runId, $schemaOut, $tableOut, $statusesFilter);

    // 2) SOMA NOT IN(statuses), se solicitado
    $not = null;
    if ($includeNotIn) {
        $not = getTotalByStatusNot($pdo, $s3, $bucket, $basePrefix, $statuses, $runId, $schemaOut, $tableOut, $statusesFilter);
    }

    // 3) TOTAL BUCKET, se solicitado (cuidado: caro/lento)
    $bucketTotal = null;
    if ($computeTotal) {
        $bucketTotal = getTotalBucketSize($s3, $bucket);
        // Grava SUMMARY de bucket
        insertResultRow($pdo, $schemaOut, $tableOut, [
            'run_id'             => $runId,
            'computed_at'        => date('Y-m-d H:i:s'),
            'scope'              => 'SUMMARY',
            'bucket'             => $bucket,
            'base_prefix'        => '',
            'codigo_acesso'      => null,
            'status'             => null,
            'prefix_base64'      => null,
            'objects_count'      => $bucketTotal['objects_count'] ?? 0,
            'bytes_total'        => (string)($bucketTotal['bytes_total'] ?? 0),
            'gb_total'           => $bucketTotal['gb_total'] ?? 0.0,
            'api_calls'          => $bucketTotal['api_calls'] ?? 0,
            'cost_estimated_usd' => $bucketTotal['cost_estimated_usd'] ?? 0.0,
            'statuses_filter'    => $statusesFilter,
            'extra_note'         => 'SUMMARY:BUCKET_TOTAL',
        ]);
    }

    // Impressão final amigável
    $totalGB = $in['gb_total'] + ($not['gb_total'] ?? 0.0);
    $totalClients = $in['processed_clients'] + ($not['processed_clients'] ?? 0);
    $totalCost = $in['cost_estimated_usd'] + ($not['cost_estimated_usd'] ?? 0.0) + ($bucketTotal['cost_estimated_usd'] ?? 0.0);

    echo PHP_EOL;
    echo "================= RESUMO =================" . PHP_EOL;
    echo "Clientes processados: {$totalClients}" . PHP_EOL;
    echo "Total (IN {$statusesFilter}): " . number_format($in['gb_total'], 2) . " GB" . PHP_EOL;
    if ($not !== null) {
        echo "Total (NOT IN {$statusesFilter}): " . number_format($not['gb_total'], 2) . " GB" . PHP_EOL;
    }
    if ($bucketTotal !== null) {
        echo "Total (BUCKET INTEIRO): " . number_format($bucketTotal['gb_total'], 2) . " GB" . PHP_EOL;
    }
    echo "Total geral (calculado): " . number_format($totalGB, 2) . " GB" . PHP_EOL;
    echo "Custo estimado desta execução: US$ " . number_format($totalCost, 6) . PHP_EOL;
    echo "Run ID: {$runId}" . PHP_EOL;
    echo "==========================================" . PHP_EOL;

    logMsg("==== Fim da execução | RunID: {$runId} | Custo total estimado: $" . number_format($totalCost, 6) . " ====");
}

// ===================== START =====================
main([
    'bucket'             => $bucket,
    'region'             => $region,
    'base_prefix'        => $basePrefix,
    'db_host'            => $dbHost,
    'db_name'            => $dbName,
    'db_schema_out'      => $dbSchemaOut,
    'table_out'          => $tableOut,
    'statuses'           => $statuses,
    'include_not_in'     => $includeNotIn,
    'compute_total_bucket'=> $computeTotalBucket,
]);