/**
 * Arrow Flight gRPC Client
 * Conecta con el Gateway Node.js y consume datos Arrow
 */
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { tableFromIPC } from 'apache-arrow';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Cargar proto de Arrow Flight
const PROTO_PATH = join(__dirname, '../proto/Flight.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

const flightProto = grpc.loadPackageDefinition(packageDefinition).arrow.flight.protocol;

export class FlightClient {
    constructor(uri = 'localhost:8815') {
        this.uri = uri;
        this.client = new flightProto.FlightService(
            uri,
            grpc.credentials.createInsecure(),
            {
                'grpc.max_receive_message_length': 200 * 1024 * 1024,
                'grpc.max_send_message_length': 200 * 1024 * 1024
            }
        );
    }

    /**
     * Obtiene metadata del dataset (GetFlightInfo)
     */
    getFlightInfo(tenantId, datasetName, rows = null) {
        return new Promise((resolve, reject) => {
            const path = [Buffer.from(tenantId), Buffer.from(datasetName)];
            if (rows) {
                path.push(Buffer.from(String(rows)));
            }

            const descriptor = {
                type: 'PATH',
                path
            };

            this.client.GetFlightInfo(descriptor, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response);
                }
            });
        });
    }

    /**
     * Obtiene datos del dataset (DoGet)
     * Retorna una promesa que resuelve a un objeto Arrow Table
     */
    doGet(ticket) {
        return new Promise((resolve, reject) => {
            const chunks = [];
            
            const call = this.client.DoGet({ ticket: ticket.ticket });
            
            call.on('data', (flightData) => {
                if (flightData.data_body && flightData.data_body.length > 0) {
                    chunks.push(flightData.data_body);
                }
            });
            
            call.on('end', () => {
                try {
                    // Concatenar todos los chunks y parsear como Arrow IPC
                    if (chunks.length === 0) {
                        resolve({ table: null, rows: 0, bytes: 0 });
                        return;
                    }
                    
                    // Cada chunk es un stream IPC completo
                    // Parseamos cada uno y combinamos
                    let totalRows = 0;
                    let totalBytes = 0;
                    const tables = [];
                    
                    for (const chunk of chunks) {
                        const table = tableFromIPC(chunk);
                        tables.push(table);
                        totalRows += table.numRows;
                        totalBytes += chunk.length;
                    }
                    
                    resolve({
                        table: tables.length === 1 ? tables[0] : null, // Simplificado
                        rows: totalRows,
                        bytes: totalBytes
                    });
                } catch (e) {
                    reject(e);
                }
            });
            
            call.on('error', (err) => {
                reject(err);
            });
        });
    }

    /**
     * Ejecuta flujo completo: GetFlightInfo + DoGet
     */
    async queryDataset(tenantId, dataset, rows = null) {
        const startTotal = performance.now();
        
        try {
            // 1. Get Flight Info
            const startMeta = performance.now();
            const info = await this.getFlightInfo(tenantId, dataset, rows);
            const metaLatency = performance.now() - startMeta;
            
            if (!info.endpoint || info.endpoint.length === 0) {
                throw new Error('No endpoints returned');
            }
            
            // 2. Do Get
            const startTransfer = performance.now();
            const result = await this.doGet(info.endpoint[0].ticket);
            const transferLatency = performance.now() - startTransfer;
            
            const totalLatency = performance.now() - startTotal;
            
            return {
                status: 'Success',
                tenant_id: tenantId,
                dataset,
                rows: result.rows,
                bytes: result.bytes,
                metadata_latency_ms: metaLatency,
                transfer_latency_ms: transferLatency,
                total_latency_ms: totalLatency
            };
            
        } catch (err) {
            return {
                status: 'Error',
                tenant_id: tenantId,
                dataset,
                error: err.message,
                total_latency_ms: performance.now() - startTotal
            };
        }
    }

    /**
     * Health check básico
     */
    async checkHealth() {
        return new Promise((resolve) => {
            const call = this.client.ListFlights({ expression: Buffer.alloc(0) });
            let healthy = false;
            
            call.on('data', () => { healthy = true; });
            call.on('end', () => resolve(true));
            call.on('error', () => resolve(false));
            
            // Timeout
            setTimeout(() => {
                call.cancel();
                resolve(healthy);
            }, 5000);
        });
    }
}
