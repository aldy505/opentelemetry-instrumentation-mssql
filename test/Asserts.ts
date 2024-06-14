import {
    SpanKind,
    Attributes,
    Span,
    SpanStatus,
} from '@opentelemetry/api';
import { TimedEvent } from '@opentelemetry/sdk-trace-base';
import * as assert from 'assert';
import { ReadableSpan } from '@opentelemetry/sdk-trace-node';
import {
    hrTimeToMilliseconds,
    hrTimeToMicroseconds,
} from '@opentelemetry/core';

export const assertSpan = (
    span: ReadableSpan,
    kind: SpanKind,
    attributes: Attributes,
    events: TimedEvent[],
    status: SpanStatus
) => {
    assert.strictEqual(span.spanContext().traceId.length, 32);
    assert.strictEqual(span.spanContext().spanId.length, 16);
    assert.strictEqual(span.kind, kind);

    assert.ok(span.endTime);
    assert.strictEqual(span.links.length, 0);

    assert.ok(
        hrTimeToMicroseconds(span.startTime) < hrTimeToMicroseconds(span.endTime)
    );
    assert.ok(hrTimeToMilliseconds(span.endTime) > 0);

    // attributes
    assert.deepStrictEqual(span.attributes, attributes);

    // events
    assert.deepStrictEqual(span.events, events);

    assert.strictEqual(span.status.code, status.code);
    if (status.message) {
        assert.strictEqual(span.status.message, status.message);
    }
};

// Check if childSpan was propagated from parentSpan
export const assertPropagation = (
    childSpan: ReadableSpan,
    parentSpan: Span
) => {
    const targetSpanContext = childSpan.spanContext;
    const sourceSpanContext = parentSpan.spanContext();
    assert.strictEqual(targetSpanContext().traceId, sourceSpanContext.traceId);
    assert.strictEqual(childSpan.parentSpanId, sourceSpanContext.spanId);
    assert.strictEqual(
        targetSpanContext().traceFlags,
        sourceSpanContext.traceFlags
    );
    assert.notStrictEqual(targetSpanContext().spanId, sourceSpanContext.spanId);
};
