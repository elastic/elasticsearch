/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.xpack.esql.core.QlClientException;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Context passed to {@link Expression#fold}.
 */
public class FoldContext {
    /**
     * {@link Expression#fold} using any amount of memory. Only safe for tests.
     */
    public static FoldContext unbounded() {
        return new FoldContext(Long.MAX_VALUE);
    }

    /**
     * {@link Expression#fold} using a small amount of memory.
     */
    public static FoldContext small() {
        return new FoldContext(Long.MAX_VALUE);
    }

    private long allowedBytes;

    public FoldContext(long allowedBytes) {
        this.allowedBytes = allowedBytes;
    }

    /**
     * Track an allocation. Best to call this <strong>before</strong> allocating
     * if possible, but after is ok if the allocation is small.
     */
    public void trackAllocation(Source source, long bytes) {
        allowedBytes -= bytes;
        if (allowedBytes < 0) {
            throw new QlFoldTooMuchMemoryException(source);
        }
    }

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
                trackAllocation(source, bytes);
            }

            @Override
            public long getUsed() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLimit() {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getOverhead() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getTrippedCount() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getName() {
                throw new UnsupportedOperationException();
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

    private static class QlFoldTooMuchMemoryException extends QlClientException {
        protected QlFoldTooMuchMemoryException(Source source) {
            super("line {}:{}: folding used too much memory", source.source().getLineNumber(), source.source().getColumnNumber());
        }
    }
}
