/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encodes {@link DocRefBlock} rows for {@link TopNOperator}. A row holds an origin ordinal, a segment and a doc id. The
 * ordinal of a {@link DocRefBlock} only means something inside its block, and a TopN mixes rows of many blocks, so a
 * row instead holds an ordinal into a {@link Registry} the operator shares with its parallel workers. Writing the whole
 * origin into every row would cost about a hundred bytes and four string encodings per row.
 * <p>
 * Planners hold {@link #PROTOTYPE}. Each operator gets its own {@link Registry} through {@link #forOperator}.
 */
public class DocRefEncoder extends DefaultUnsortableTopNEncoder {
    public static final DocRefEncoder PROTOTYPE = new DocRefEncoder();

    private DocRefEncoder() {}

    @Override
    public TopNEncoder forOperator(CircuitBreaker breaker) {
        // parallel workers intern from their own threads, and a LocalCircuitBreaker only takes calls from its driver
        return new Registry(LocalCircuitBreaker.forAsyncIo(breaker));
    }

    /**
     * The ordinal of {@code origin} in the registry of the operator, interning it on first sight.
     */
    int intern(DocRefOrigin origin) {
        throw new IllegalStateException("encode document references with the encoder from forOperator");
    }

    /**
     * The origin an ordinal from {@link #intern} stands for.
     */
    DocRefOrigin origin(int ordinal) {
        throw new IllegalStateException("decode document references with the encoder from forOperator");
    }

    @Override
    public String toString() {
        return "DocRef";
    }

    /**
     * The origins of every row one operator tree has encoded. The workers of a parallel TopN intern concurrently and
     * the merge target decodes their rows, so lookups by origin go through a {@link ConcurrentHashMap} and the first
     * sight of an origin takes a lock. The dictionary only grows, and the operator returns its bytes when it closes.
     */
    static final class Registry extends DocRefEncoder implements Releasable, Accountable {
        // the map entry, the boxed ordinal and the array slot of one origin
        private static final long ENTRY_BYTES = 64 + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF;

        private final CircuitBreaker breaker;
        private final Map<DocRefOrigin, Integer> ordinals = new ConcurrentHashMap<>();
        // written under the lock, published to the threads that decode by the volatile write
        private volatile DocRefOrigin[] origins = new DocRefOrigin[4];
        private int size;
        private long reservedBytes;
        private boolean closed;

        private Registry(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        int intern(DocRefOrigin origin) {
            Integer ordinal = ordinals.get(origin);
            if (ordinal != null) {
                return ordinal;
            }
            synchronized (this) {
                ordinal = ordinals.get(origin);
                if (ordinal != null) {
                    return ordinal;
                }
                if (closed) {
                    // a parallel worker can still run after its owner closed, and must not reserve bytes nobody returns
                    throw new IllegalStateException("the TopN that owns this registry is closed");
                }
                long bytes = origin.ramBytesUsed() + ENTRY_BYTES;
                breaker.addEstimateBytesAndMaybeBreak(bytes, "topn");
                reservedBytes += bytes;
                DocRefOrigin[] current = origins;
                if (size == current.length) {
                    current = Arrays.copyOf(current, size * 2);
                }
                current[size] = origin;
                origins = current;
                ordinals.put(origin, size);
                return size++;
            }
        }

        @Override
        DocRefOrigin origin(int ordinal) {
            return origins[ordinal];
        }

        @Override
        public TopNEncoder forOperator(CircuitBreaker breaker) {
            throw new IllegalStateException("a registry belongs to one operator");
        }

        @Override
        public synchronized long ramBytesUsed() {
            return reservedBytes;
        }

        @Override
        public synchronized void close() {
            if (closed == false) {
                closed = true;
                breaker.addWithoutBreaking(-reservedBytes);
                reservedBytes = 0;
            }
        }
    }
}
