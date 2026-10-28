/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.QlClientException;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Context passed to {@link Expression#fold}. This is not thread safe.
 */
public class FoldContext {
    private static final long SMALL = MemorySizeValue.parseBytesSizeValueOrHeapRatio("5%", "small").getBytes();

    /**
     * {@link Expression#fold} using less than 5% of heap. Fine in tests but otherwise
     * calling this is a signal that you either, shouldn't be calling {@link Expression#fold}
     * at all, or should pass in a shared {@link FoldContext} made by {@code Configuration}.
     */
    public static FoldContext small() {
        return new FoldContext(SMALL);
    }

    private final long initialAllowedBytes;
    private long allowedBytes;

    public FoldContext(long allowedBytes) {
        this.initialAllowedBytes = allowedBytes;
        this.allowedBytes = allowedBytes;
    }

    /**
     * The maximum allowed bytes. {@link #allowedBytes()} will be the same as this
     * for an unused context.
     */
    public long initialAllowedBytes() {
        return initialAllowedBytes;
    }

    /**
     * The remaining allowed bytes.
     */
    long allowedBytes() {
        return allowedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FoldContext that = (FoldContext) o;
        return initialAllowedBytes == that.initialAllowedBytes && allowedBytes == that.allowedBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialAllowedBytes, allowedBytes);
    }

    @Override
    public String toString() {
        return "FoldContext[" + allowedBytes + '/' + initialAllowedBytes + ']';
    }

    /**
     * Track an allocation. Best to call this <strong>before</strong> allocating
     * if possible, but after is ok if the allocation is small.
     * <p>
     *     Note that, unlike {@link CircuitBreaker}, you don't <strong>have</strong>
     *     to free this allocation later. This is important because the query plan
     *     doesn't implement {@link Releasable} so it <strong>can't</strong> free
     *     consistently. But when you have to allocate big chunks of memory during
     *     folding and know that you are returning the memory it is kindest to
     *     call this with a negative number, effectively giving those bytes back.
     * </p>
     */
    public void trackAllocation(Source source, long bytes) {
        allowedBytes -= bytes;
        assert allowedBytes <= initialAllowedBytes : "returned more bytes than it used";
        if (allowedBytes < 0) {
            throw new FoldTooMuchMemoryException(source, bytes, initialAllowedBytes);
        }
    }

    /**
     * Adapt this into a {@link CircuitBreaker} suitable for building bounded local
     * DriverContext. This is absolutely an abuse of the {@link CircuitBreaker} contract
     * and only methods used by BlockFactory are implemented. And this'll throw a
     * {@link FoldTooMuchMemoryException} instead of the standard {@link CircuitBreakingException}.
     * This works for the common folding implementation though.
     */
    public CircuitBreaker circuitBreakerView(Source source) {
        return new CircuitBreaker() {
            @Override
            public void circuitBreak(String fieldName, long bytesNeeded) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                trackAllocation(source, bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                assert bytes <= 0 : "we only expect this to be used for deallocation";
                allowedBytes -= bytes;
                assert allowedBytes <= initialAllowedBytes : "returned more bytes than it used";
            }

            @Override
            public long getUsed() {
                /*
                 * This isn't expected to be used by we can implement it so we may as
                 * well. Maybe it'll be useful for debugging one day.
                 */
                return initialAllowedBytes - allowedBytes;
            }

            @Override
            public long getLimit() {
                /*
                 * This isn't expected to be used by we can implement it so we may as
                 * well. Maybe it'll be useful for debugging one day.
                 */
                return initialAllowedBytes;
            }

            @Override
            public double getOverhead() {
                return 1.0;
            }

            @Override
            public long getTrippedCount() {
                return 0;
            }

            @Override
            public String getName() {
                return REQUEST;
            }

            @Override
            public Durability getDurability() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setLimitAndOverhead(long limit, double overhead) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static class FoldTooMuchMemoryException extends QlClientException {
        protected FoldTooMuchMemoryException(Source source, long bytesForExpression, long initialAllowedBytes) {
            super(
                "line {}:{}: Folding query used more than {}. The expression that pushed past the limit is [{}] which needed {}.",
                source.source().getLineNumber(),
                source.source().getColumnNumber(),
                ByteSizeValue.ofBytes(initialAllowedBytes),
                source.text(),
                ByteSizeValue.ofBytes(bytesForExpression)
            );
        }
    }
}
