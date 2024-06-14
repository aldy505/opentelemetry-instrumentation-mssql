import { describe, it, assert, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { context, trace } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks';
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';

import { MssqlInstrumentation } from '../src';

const instrumentation = new MssqlInstrumentation({});
import mssql from 'mssql';

const config: mssql.config = {
  user: process.env.MSSQL_USER || 'sa',
  password: process.env.MSSQL_PASSWORD || 'P@ssw0rd',
  server: process.env.MSSQL_HOST || 'localhost',  
  database: process.env.MSSQL_DATABASE || 'tempdb',
  port: 1433,
  options: {
    enableArithAbort: true,
    encrypt: false
  }
};

const connectionString = `Server=${config.server};Database=${config.database};User Id=${config.user};Password=${config.password};Encrypt=${config.options?.encrypt ?? false}`;

describe.sequential('mssql@10.x', () => {
  const testMssql = process.env.RUN_MSSQL_TESTS || true; // For CI: assumes local mysql db is already available
  const testMssqlLocally = process.env.RUN_MSSQL_TESTS_LOCAL; // For local: spins up local mysql db via docker
  const shouldTest = testMssql || testMssqlLocally; // Skips these tests if false (default)

  let contextManager: AsyncHooksContextManager;

  const provider = new NodeTracerProvider();
  const memoryExporter = new InMemorySpanExporter();
  let pool: mssql.ConnectionPool;

  beforeAll(() => {
      instrumentation.setTracerProvider(provider);
      provider.addSpanProcessor(new SimpleSpanProcessor(memoryExporter));
      if (testMssqlLocally) {
        console.log('Starting mssql...');
      }
  });

  afterAll(function () {
    if (testMssqlLocally) {
      console.log('Stopping mssql...');
    }
  });

  beforeEach(async () => {
      contextManager = new AsyncHooksContextManager();
      context.setGlobalContextManager(contextManager.enable());
      instrumentation.enable();
      
      //start pool        
      pool = new mssql.ConnectionPool(config, (err) => {
        if (err) {
          console.debug("SQL Connection Establishment ERROR: %s", err);
        } else {
          console.debug('SQL Connection established...');
        }
      });
      await pool.connect()
      pool.on('error', err => {
        console.error(err);
      });
      
    });
  
    afterEach(async () => {
      memoryExporter.reset();
      contextManager.disable();
      instrumentation.disable();

      // end pool
      await pool.close();
    });

    it('should export a plugin', () => {
      assert(instrumentation instanceof MssqlInstrumentation);
    });
  
    it('should have correct moduleName', () => {
      assert.strictEqual(instrumentation.instrumentationName, 'opentelemetry-instrumentation-mssql');
    });
    
    describe('when the query is a string', () => {
      it('should name the span accordingly', async () => {
        const span = provider.getTracer('default').startSpan('test span');
        context.with(trace.setSpan(context.active(), span), () => {
          const request = new mssql.Request(pool);
          request.query('SELECT 1 as number').finally(() => {
            span.end();
            const spans = memoryExporter.getFinishedSpans();
            console.log(spans)
            assert.strictEqual(spans[0].name, 'SELECT');
          });
        });
      });
    });

    describe('when connectionString is provided', () => {
      it('should name the span accordingly ', async () => {
          const pool = new mssql.ConnectionPool(connectionString)
          await pool.connect();
          const request = new mssql.Request(pool);
          request.query(`SELECT 1 as number`).then((result) => {
            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans[0].name, 'SELECT');
          });
          
          await pool.close();
      });
    });

    describe('when connectionString is provided for query on pool', () => {
      it('should name the span accordingly, query on pool', async () => {          
          const pool = new mssql.ConnectionPool(connectionString)
          
          await pool.connect();
          await pool.query`SELECT 1 as number`;
          const spans = memoryExporter.getFinishedSpans();
          console.log(spans)
          assert.strictEqual(spans[0].name, 'SELECT');
          await pool.close();
      });
    });

    describe('when connectionString is provided for query on pool', () => {
      it('should name the span accordingly, query on pool.request', async () => {          
          const pool = new mssql.ConnectionPool(connectionString)
          await pool.connect();
          pool.request().query(`SELECT 1 as number`).then((result) => {
            const spans = memoryExporter.getFinishedSpans();
            assert.strictEqual(spans[0].name, 'SELECT');
          });
          await pool.close();
      });
    });

});