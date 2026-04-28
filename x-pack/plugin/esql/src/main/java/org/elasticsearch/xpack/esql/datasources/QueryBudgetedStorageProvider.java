/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Decorates a {@link StorageProvider} with per-query concurrency budget enforcement. Each
 * {@link StorageObject} created by this provider is wrapped with a {@link QueryBudgetedStorageObject}
 * that acquires from the query's {@link QueryConcurrencyBudget} before each I/O operation.
 * <p>
 * Implements {@link java.io.Closeable} to release the query's budget when the query completes.
 * Closing this provider closes the underlying budget, which deregisters from the
 * {@link ConcurrencyBudgetAllocator} and triggers rebalancing of remaining active queries.
 */
class QueryBudgetedStorageProvider implements StorageProvider {

    private final StorageProvider delegate;
    private final QueryConcurrencyBudget budget;

    QueryBudgetedStorageProvider(StorageProvider delegate, QueryConcurrencyBudget budget) {
        this.delegate = delegate;
        this.budget = budget;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new QueryBudgetedStorageObject(delegate.newObject(path), budget);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new QueryBudgetedStorageObject(delegate.newObject(path, length), budget);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new QueryBudgetedStorageObject(delegate.newObject(path, length, lastModified), budget);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        return delegate.listObjects(prefix, recursive);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public List<String> supportedSchemes() {
        return delegate.supportedSchemes();
    }

    /**
     * Closes the per-query budget only; the delegate provider is shared (registry-owned) and
     * intentionally not closed here.
     */
    @Override
    public void close() throws IOException {
        budget.close();
    }
}
