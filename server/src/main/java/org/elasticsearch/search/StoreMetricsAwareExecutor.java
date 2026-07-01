/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.index.store.StoreMetrics;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Wrapping executor to count for storemetrics bytes read and return the sum of all bytes read within the executor
 */
public class StoreMetricsAwareExecutor implements Executor {

    private final Executor executor;
    private final Supplier<StoreMetrics> storeMetricsSupplier;
    private final LongAdder workerBytesRead;

    public StoreMetricsAwareExecutor(Executor executor, Supplier<StoreMetrics> storeMetricsSupplier) {
        this.executor = executor;
        this.storeMetricsSupplier = storeMetricsSupplier;
        this.workerBytesRead = new LongAdder();
    }

    @Override
    public void execute(Runnable runnable) {
        executor.execute(() -> {
            final StoreMetrics workerStoreMetrics = storeMetricsSupplier.get();
            final long before = workerStoreMetrics.getBytesRead();
            try {
                runnable.run();
            } finally {
                workerBytesRead.add(workerStoreMetrics.getBytesRead() - before);
            }
        });
    }

    // bytes captured from worker threads forked through this executor
    public long workerBytesRead() {
        return workerBytesRead.sum();
    }
}
