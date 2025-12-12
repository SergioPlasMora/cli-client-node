/**
 * Arrow Flight CLI Client - Node.js
 * Entry point
 */
import { program } from 'commander';
import { FlightClient } from './flight-client.js';
import { LoadTester } from './load-tester.js';
import Table from 'cli-table3';

const GATEWAY_URI = 'localhost:8815';

// Banner
function printBanner() {
    console.log('╭─────────────────────────────────╮');
    console.log('│ Arrow Flight Client (Node.js)   │');
    console.log('│ Multi-tenant Load Tester        │');
    console.log('╰─────────────────────────────────╯');
    console.log('');
}

// Comando: query
async function cmdQuery(tenant, dataset, options) {
    printBanner();
    console.log(`Running single query for tenant: ${tenant}, dataset: ${dataset}`);
    console.log('');

    const client = new FlightClient(options.gateway);
    
    const healthy = await client.checkHealth();
    if (!healthy) {
        console.error(`Error: Could not connect to Gateway at ${options.gateway}`);
        process.exit(1);
    }

    const result = await client.queryDataset(tenant, dataset, options.rows);
    
    if (result.status === 'Success') {
        const table = new Table();
        table.push(
            { 'Status': result.status },
            { 'Rows': result.rows.toLocaleString() },
            { 'Bytes': `${(result.bytes / 1024 / 1024).toFixed(2)} MB` },
            { 'Metadata Latency': `${result.metadata_latency_ms.toFixed(2)} ms` },
            { 'Transfer Latency': `${result.transfer_latency_ms.toFixed(2)} ms` },
            { 'Total Latency': `${result.total_latency_ms.toFixed(2)} ms` }
        );
        console.log(table.toString());
    } else {
        console.error(`Error: ${result.error}`);
    }
}

// Comando: load-test
async function cmdLoadTest(options) {
    printBanner();
    
    const tenants = options.tenantsList 
        ? options.tenantsList.split(',') 
        : Array.from({ length: options.tenantsCount }, (_, i) => `tenant_${String(i + 1).padStart(3, '0')}`);

    console.log('Starting Load Test:');
    console.log(`  Requests:    ${options.requests}`);
    console.log(`  Concurrency: ${options.concurrency}`);
    console.log(`  Tenants:     ${tenants.length} (${tenants.slice(0, 3).join(', ')}...)`);
    console.log(`  Gateway:     grpc://${options.gateway}`);
    console.log('-'.repeat(40));
    console.log('');

    const tester = new LoadTester(options.gateway, options.concurrency);
    const startTime = Date.now();
    
    // Progress indicator
    const progressInterval = setInterval(() => {
        process.stdout.write(`\r  Progress: ${tester.results.length}/${options.requests} requests...`);
    }, 500);

    const metrics = await tester.runLoadTest(
        options.requests, 
        tenants, 
        options.dataset, 
        options.rows
    );

    clearInterval(progressInterval);
    console.log('\r' + ' '.repeat(50) + '\r');

    // Mostrar resultados
    console.log('Load Test Results');
    
    const summaryTable = new Table({
        head: ['Metric', 'Value'],
        style: { head: ['cyan'] }
    });
    summaryTable.push(
        ['Duration', `${metrics.duration_s.toFixed(2)} s`],
        ['Total Requests', metrics.total_requests],
        ['Successful', metrics.successful],
        ['Failed', metrics.failed],
        ['Throughput', `${metrics.throughput.toFixed(2)} req/s`],
        ['Total Data', `${(metrics.total_bytes / 1024 / 1024).toFixed(2)} MB`]
    );
    console.log(summaryTable.toString());

    const latencyTable = new Table({
        head: ['Statistic', 'Time (ms)'],
        style: { head: ['cyan'] }
    });
    latencyTable.push(
        ['Average', metrics.avg_latency_ms.toFixed(2)],
        ['P95', metrics.p95_latency_ms.toFixed(2)]
    );
    console.log(latencyTable.toString());

    // Exportar JSON si se pide
    if (options.json) {
        const fs = await import('fs');
        fs.writeFileSync(options.json, JSON.stringify(metrics, null, 2));
        console.log(`\nResults exported to ${options.json}`);
    }
}

// CLI setup
program
    .name('arrow-flight-client')
    .description('Arrow Flight Client for Node.js')
    .version('1.0.0');

program
    .command('query')
    .description('Execute a single query')
    .argument('<tenant>', 'Tenant ID')
    .argument('<dataset>', 'Dataset name')
    .option('-g, --gateway <uri>', 'Gateway URI', GATEWAY_URI)
    .option('-r, --rows <number>', 'Number of rows', parseInt)
    .action(cmdQuery);

program
    .command('load-test')
    .description('Run load test')
    .option('-r, --requests <number>', 'Total requests', (val) => parseInt(val, 10), 100)
    .option('-c, --concurrency <number>', 'Concurrent workers', (val) => parseInt(val, 10), 10)
    .option('-t, --tenants-count <number>', 'Number of simulated tenants', (val) => parseInt(val, 10), 5)
    .option('-T, --tenants-list <list>', 'Comma-separated list of tenant IDs')
    .option('-d, --dataset <name>', 'Dataset name', 'sales')
    .option('--rows <number>', 'Rows per request', (val) => parseInt(val, 10))
    .option('-g, --gateway <uri>', 'Gateway URI', GATEWAY_URI)
    .option('-j, --json <file>', 'Export results to JSON')
    .action(cmdLoadTest);

program.parse();
