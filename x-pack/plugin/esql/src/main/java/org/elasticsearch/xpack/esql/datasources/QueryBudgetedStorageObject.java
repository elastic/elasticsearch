/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a {@link StorageObject} with per-query concurrency budget enforcement. Each I/O
 * operation acquires a permit from the query's {@link QueryConcurrencyBudget} before delegating
 * and releases it when the operation completes. For stream-returning methods, the permit is
 * held until the stream is closed.
 * <p>
 * This wrapper is applied on top of the global {@link ConcurrencyLimitedStorageObject} to provide
 * two-level concurrency control: per-query fairness (this layer) + global hard cap (inner layer).
 */
class QueryBudgetedStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final QueryConcurrencyBudget budget;

    QueryBudgetedStorageObject(StorageObject delegate, QueryConcurrencyBudget budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public InputStream newStream() throws IOException {
        acquirePermit();
        try {
            InputStream stream = delegate.newStream();
            return new PermitReleasingInputStream(stream, budget);
        } catch (Exception e) {
            budget.release();
            throw e;
        }
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        acquirePermit();
        try {
            InputStream stream = delegate.newStream(position, length);
            return new PermitReleasingInputStream(stream, budget);
        } catch (Exception e) {
            budget.release();
            throw e;
        }
    }

    @Override
    public long length() throws IOException {
        acquirePermit();
        try {
            return delegate.length();
        } finally {
            budget.release();
        }
    }

    @Override
    public Instant lastModified() throws IOException {
        acquirePermit();
        try {
            return delegate.lastModified();
        } finally {
            budget.release();
        }
    }

    @Override
    public boolean exists() throws IOException {
        acquirePermit();
        try {
            return delegate.exists();
        } finally {
            budget.release();
        }
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        acquirePermit();
        try {
            return delegate.readBytes(position, target);
        } finally {
            budget.release();
        }
    }

    @Override
    public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
        try {
            acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            delegate.readBytesAsync(position, length, executor, ActionListener.wrap(result -> {
                budget.release();
                listener.onResponse(result);
            }, e -> {
                budget.release();
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            budget.release();
            listener.onFailure(e);
        }
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        try {
            acquirePermit();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try {
            delegate.readBytesAsync(position, target, executor, ActionListener.wrap(result -> {
                budget.release();
                listener.onResponse(result);
            }, e -> {
                budget.release();
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            budget.release();
            listener.onFailure(e);
        }
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    private void acquirePermit() {
        try {
            budget.acquire();
        } catch (TimeoutException e) {
            throw new EsRejectedExecutionException("Failed to acquire query concurrency budget permit: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EsRejectedExecutionException("Interrupted while waiting for query concurrency budget permit: " + e);
        }
    }

    private static class PermitReleasingInputStream extends FilterInputStream {
        private final QueryConcurrencyBudget budget;
        private boolean released;

        PermitReleasingInputStream(InputStream in, QueryConcurrencyBudget budget) {
            super(in);
            this.budget = budget;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                if (released == false) {
                    released = true;
                    budget.release();
                }
            }
        }
    }
}
