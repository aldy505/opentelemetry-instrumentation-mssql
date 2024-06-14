import { SEMATTRS_DB_SYSTEM, SEMATTRS_DB_STATEMENT } from '@opentelemetry/semantic-conventions';
import {
    InstrumentationBase,
    InstrumentationConfig,
    InstrumentationNodeModuleDefinition,
    isWrapped
} from '@opentelemetry/instrumentation';

import {
    SpanKind,
    SpanStatusCode,
    trace,
    context,
    diag
} from '@opentelemetry/api';

import type * as mssqlTypes from "mssql";
import mssql from 'mssql';
import { MssqlInstrumentationConfig } from './types';
import { getConnectionAttributes, getSpanName } from './Spans';
import { VERSION } from './version';

type Config = InstrumentationConfig & MssqlInstrumentationConfig;

export class MssqlInstrumentation extends InstrumentationBase {

    static readonly COMPONENT = 'mssql';
    static readonly COMMON_ATTRIBUTES = {
        [SEMATTRS_DB_SYSTEM]: MssqlInstrumentation.COMPONENT,
    };

    constructor(config: Config = {}) {
        super('opentelemetry-instrumentation-mssql', VERSION, Object.assign({}, config));
    }

    private _getConfig(): MssqlInstrumentationConfig {
        return this._config as MssqlInstrumentationConfig;
    }

    protected init() {
        const module = new InstrumentationNodeModuleDefinition(
            MssqlInstrumentation.COMPONENT,
            ['10.*'],
            (module: any) => {
                const moduleExports: typeof mssqlTypes = module[Symbol.toStringTag] === 'Module'
                  ? module.default // ESM
                  : module; // CommonJS

                  this._wrap(moduleExports, 'ConnectionPool', this._patchCreatePool() as any);
                  this._wrap(moduleExports, 'Request', this._patchRequest() as any);
            },
            (module: any) => {
                const moduleExports: typeof mssqlTypes = module[Symbol.toStringTag] === 'Module'
                  ? module.default // ESM
                  : module; // CommonJS

                if (isWrapped(moduleExports.ConnectionPool)) {
                    this._unwrap(moduleExports, 'ConnectionPool');
                }
    
                if (isWrapped(moduleExports.Request)) {
                    this._unwrap(moduleExports, 'Request');
                }
            }
        );

        return module;
    }

    // global export function
    private _patchCreatePool() {
        const plugin = this;
        return (originalConnectionPool: mssqlTypes.ConnectionPool) => {
            diag.debug('MssqlPlugin#patch: patching mssql ConnectionPool');
            return function createPool(_config: string | mssql.config) {
                if (plugin._getConfig()?.ignoreOrphanedSpans && !trace.getSpan(context.active())) {
                    return originalConnectionPool;
                }
                const pool = originalConnectionPool;
                plugin._wrap(pool, 'query', plugin._patchPoolQuery(pool));
                plugin._wrap(pool, 'request', plugin._patchRequest());
                return pool;
            };
        };
    }

    private _patchPoolQuery(pool: mssqlTypes.ConnectionPool) {
        const plugin = this;
        return (originalQuery: typeof mssqlTypes.ConnectionPool.prototype.query) => {
            console.log("hello there")
            diag.debug('MssqlPlugin#patch: patching mssql pool request');
            return function query(this: mssqlTypes.ConnectionPool, ...args: unknown[]) {
                if (plugin.shouldIgnoreOrphanSpans(plugin._getConfig())) {
                    return originalQuery.apply(this, args as never);
                }

                const arg0 = args[0] as string | TemplateStringsArray;
                return plugin.tracer.startActiveSpan(getSpanName(arg0), {
                    kind: SpanKind.CLIENT,
                    attributes: {
                        ...MssqlInstrumentation.COMMON_ATTRIBUTES,
                        ...getConnectionAttributes((<any>pool).config)
                    }
                }, (span) => {
                    try {
                        return originalQuery.apply(pool, args as never);
                    } catch (error: unknown) {
                        if (typeof error === "object" && error != null && "message" in error && typeof error.message === "string") {
                            span.setStatus({
                                code: SpanStatusCode.ERROR,
                                message: error.message,
                            });
                        }
                        
                        throw error;
                    }
                });               

            };
        };
    }

    private _patchRequest() {
        return (originalRequest: any) => {
            const thisInstrumentation = this;
            diag.debug('MssqlPlugin#patch: patching mssql pool request');
            return function request() {
                const request: mssql.Request = new originalRequest(...arguments);
                thisInstrumentation._wrap(request, 'query', thisInstrumentation._patchQuery(request));
                return request;
            };
        };
    }

    private _patchQuery(request: mssql.Request) {
        return (originalQuery: Function) => {
            const thisInstrumentation = this;
            console.log("obi wan")
            diag.debug('MssqlPlugin#patch: patching mssql request query');
            return function query(command: string | TemplateStringsArray): Promise<mssql.IResult<any>> {
                if (thisInstrumentation.shouldIgnoreOrphanSpans(thisInstrumentation._getConfig())) {
                    return originalQuery.apply(request, arguments);
                }
                
                return thisInstrumentation.tracer.startActiveSpan(getSpanName(command), {
                    kind: SpanKind.CLIENT,
                    attributes: {
                        ...MssqlInstrumentation.COMMON_ATTRIBUTES,
                        ...getConnectionAttributes((<any>request).parent!.config)
                    },
                }, async (span) => {
                    let interpolated = thisInstrumentation.formatDbStatement(command)
                    for (const property in request.parameters) {
                        interpolated = interpolated.replace(`@${property}`, `${(request.parameters[property].value)}`);
                    }
                    span.setAttribute(SEMATTRS_DB_STATEMENT, interpolated);
                    const result = originalQuery.apply(request, arguments);

                    result
                        .catch((error: { message: any; }) => {
                            span.setStatus({
                                code: SpanStatusCode.ERROR,
                                message: error.message,
                            })
                        });
    
                    return result;
                });
            };
        };
    }

    private shouldIgnoreOrphanSpans(config: MssqlInstrumentationConfig) {
        return config?.ignoreOrphanedSpans && !trace.getSpan(context.active())
    }

    private formatDbStatement(command: string | TemplateStringsArray) {
        if (typeof command === 'object') {
            return command[0];
        }
        return command;
    }
}