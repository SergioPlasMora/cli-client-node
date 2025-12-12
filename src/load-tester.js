/**
 * Load Tester - Generador de carga concurrente
 * Equivalente a load_tester.py de Python
 */
import { FlightClient } from './flight-client.js';

export class LoadTester {
    constructor(gatewayUri = 'localhost:8815', concurrency = 10) {
        this.gatewayUri = gatewayUri;
        this.concurrency = concurrency;
        this.results = [];
    }

    /**
     * Ejecuta una sola request
     */
    async singleRequest(tenantId, dataset, rows = null) {
        const client = new FlightClient(this.gatewayUri);
        return await client.queryDataset(tenantId, dataset, rows);
    }

    /**
     * Ejecuta prueba de carga con concurrencia limitada
     */
    async runLoadTest(totalRequests, tenants, dataset = 'sales', rows = null) {
        const startTime = performance.now();
        this.results = [];

        // Crear pool de promesas con límite de concurrencia
        const pending = new Set();
        const allPromises = [];

        for (let i = 0; i < totalRequests; i++) {
            const tenant = tenants[i % tenants.length];
            
            const promise = (async () => {
                const result = await this.singleRequest(tenant, dataset, rows);
                this.results.push(result);
                return result;
            })();

            pending.add(promise);
            allPromises.push(promise);
            
            // Limpiar promesas completadas
            promise.finally(() => pending.delete(promise));

            // Esperar si alcanzamos el límite de concurrencia
            if (pending.size >= this.concurrency) {
                await Promise.race(pending);
            }
        }

        // Esperar que terminen todas
        await Promise.all(allPromises);

        const duration = (performance.now() - startTime) / 1000;
        return this.calculateMetrics(duration);
    }

    /**
     * Calcula estadísticas finales
     */
    calculateMetrics(duration) {
        const successful = this.results.filter(r => r.status === 'Success');
        const failed = this.results.filter(r => r.status !== 'Success');

        const latencies = successful
            .map(r => r.total_latency_ms)
            .sort((a, b) => a - b);

        const totalRows = successful.reduce((sum, r) => sum + (r.rows || 0), 0);
        const totalBytes = successful.reduce((sum, r) => sum + (r.bytes || 0), 0);

        const avgLatency = latencies.length > 0 
            ? latencies.reduce((a, b) => a + b, 0) / latencies.length 
            : 0;
        const p95Latency = latencies.length > 0 
            ? latencies[Math.floor(latencies.length * 0.95)] 
            : 0;

        return {
            total_requests: this.results.length,
            successful: successful.length,
            failed: failed.length,
            total_rows: totalRows,
            total_bytes: totalBytes,
            duration_s: duration,
            throughput: this.results.length / duration,
            avg_latency_ms: avgLatency,
            p95_latency_ms: p95Latency
        };
    }
}
