/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.index.store.DirectoryMetrics;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wrapping executor that captures each forked task's {@link DirectoryMetrics} on the worker thread that runs it and
 * exposes the aggregated total.
 */
public class DirectoryMetricsAwareExecutor implements Executor {

    private final Executor executor;
    private final DirectoryMetrics.Capture metricsCaptureSupplier;
    private final AtomicReference<DirectoryMetrics> workerMetrics = new AtomicReference<>(DirectoryMetrics.EMPTY);

    public DirectoryMetricsAwareExecutor(Executor executor, DirectoryMetrics.Capture metricsCaptureSupplier) {
        this.executor = executor;
        this.metricsCaptureSupplier = metricsCaptureSupplier;
    }

    @Override
    public void execute(Runnable runnable) {
        executor.execute(() -> metricsCaptureSupplier.measure(runnable, workerMetrics));
    }

    public DirectoryMetrics workerMetrics() {
        return workerMetrics.get();
    }
}
