/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.AbstractRefCounted;

/** One admitted byte copy shared between a pending row and independently surviving completed output. */
final class SequencePayloadHandle extends AbstractRefCounted {
    private final CircuitBreaker breaker;
    private final long bytes;
    private BytesRef value;

    private SequencePayloadHandle(CircuitBreaker breaker, long bytes, BytesRef value) {
        this.breaker = breaker;
        this.bytes = bytes;
        this.value = value;
    }

    static SequencePayloadHandle copyOf(BytesRef source, CircuitBreaker breaker) {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(SequencePayloadHandle.class) + RamUsageEstimator.shallowSizeOfInstance(
            BytesRef.class
        ) + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) source.length);
        breaker.addEstimateBytesAndMaybeBreak(bytes, "esql_sequence_payload");
        try {
            return new SequencePayloadHandle(breaker, bytes, BytesRef.deepCopyOf(source));
        } catch (RuntimeException | Error e) {
            breaker.addWithoutBreaking(-bytes, "esql_sequence_payload");
            throw e;
        }
    }

    BytesRef bytes() {
        if (hasReferences() == false) {
            throw new IllegalStateException("sequence payload handle is closed");
        }
        return value;
    }

    long accountedBytes() {
        return bytes;
    }

    @Override
    protected void closeInternal() {
        value = null;
        breaker.addWithoutBreaking(-bytes, "esql_sequence_payload");
    }
}
