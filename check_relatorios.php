#!/usr/bin/env php
<?php
/**
 * check_relatorios.php
 * Lê clientes do RDS (medic_clinica), verifica se existe a pasta
 * uploads/{base64(codigo_acesso)}/usuarios/relatorios/ no bucket S3
 * e imprime um JSON com os codigo_acesso que possuem essa pasta.
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Aws\S3\S3Client;
use Aws\Exception\AwsException;
use Dotenv\Dotenv;

// ===================== CONFIG =====================
$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();
$dotenv->required(['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASS']);

$region  = $_ENV['S3_REGION']      ?? 'sa-east-1';
$bucket  = $_ENV['CDN_BUCKET']     ?? '4medic-cdn';

$dbHost  = $_ENV['DB_HOST'];
$dbName  = $_ENV['DB_NAME'];
$dbUser  = $_ENV['DB_USER'];
$dbPass  = $_ENV['DB_PASS'];

$statusesStr = $_ENV['STATUSES'] ?? 'Y,B,S,G';
$statuses    = array_values(array_filter(array_map('trim', explode(',', $statusesStr))));

// ===================== CONEXÕES =====================
function connectDatabase(string $host, string $db, string $user, string $pass): PDO {
    $dsn  = "mysql:host={$host};dbname={$db};charset=utf8mb4";
    $opts = [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ];
    return new PDO($dsn, $user, $pass, $opts);
}

function createS3Client(string $region): S3Client {
    return new S3Client(['version' => 'latest', 'region' => $region]);
}

// ===================== LÓGICA =====================

/**
 * Retorna true se o prefixo existir no bucket (pelo menos 1 objeto).
 * Usa MaxKeys=1 para minimizar custo (1 chamada por cliente).
 */
function prefixExists(S3Client $s3, string $bucket, string $prefix): bool {
    try {
        $result = $s3->listObjectsV2([
            'Bucket'  => $bucket,
            'Prefix'  => $prefix,
            'MaxKeys' => 1,
        ]);
        return ($result['KeyCount'] ?? 0) > 0;
    } catch (AwsException $e) {
        fwrite(STDERR, "ERRO S3 [{$prefix}]: " . $e->getAwsErrorMessage() . PHP_EOL);
        return false;
    }
}

/**
 * Busca todos os clientes ativos do RDS (codigo_acesso 6 dígitos).
 */
function fetchClients(PDO $pdo, array $statuses): array {
    $in   = implode(',', array_fill(0, count($statuses), '?'));
    $sql  = "SELECT codigo_acesso
               FROM medic_clinica
              WHERE status IN ($in)
                AND codigo_acesso BETWEEN 100000 AND 999999
                AND codigo_acesso IS NOT NULL
                AND codigo_acesso <> 0
           ORDER BY codigo_acesso";

    $stmt = $pdo->prepare($sql);
    foreach ($statuses as $i => $st) {
        $stmt->bindValue($i + 1, $st);
    }
    $stmt->execute();
    return $stmt->fetchAll(PDO::FETCH_COLUMN);
}

// ===================== MAIN =====================
$pdo = connectDatabase($dbHost, $dbName, $dbUser, $dbPass);
$s3  = createS3Client($region);

$clients = fetchClients($pdo, $statuses);
$total   = count($clients);

fwrite(STDERR, "Verificando {$total} clientes no bucket [{$bucket}]..." . PHP_EOL);

$found = [];
foreach ($clients as $i => $codigo) {
    $b64    = base64_encode((string)$codigo);
    $prefix = "uploads/{$b64}/usuarios/relatorios/";

    if (prefixExists($s3, $bucket, $prefix)) {
        $found[] = (string)$codigo;
    }

    // Progresso a cada 100 clientes
    if (($i + 1) % 100 === 0) {
        fwrite(STDERR, sprintf("  %d/%d verificados, %d encontrados até agora...\n", $i + 1, $total, count($found)));
    }
}

fwrite(STDERR, sprintf("Concluído: %d/%d clientes com pasta relatorios.\n", count($found), $total));

echo json_encode($found, JSON_PRETTY_PRINT) . PHP_EOL;
