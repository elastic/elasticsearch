/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import java.util.function.Supplier;

public class ThreadLocalDirectoryMetricHolder<M extends DirectoryMetrics.PluggableMetrics<M>>
    implements
        PluggableDirectoryMetricsHolder<M> {
    private final Supplier<? extends M> metricsSupplier;
    private final ThreadLocal<M> threadLocal = new ThreadLocal<>() {
        @Override
        protected M initialValue() {
            return metricsSupplier.get();
        }
    };

    public ThreadLocalDirectoryMetricHolder(Supplier<? extends M> metricsSupplier) {
        this.metricsSupplier = metricsSupplier;
    }

    @Override
    public PluggableDirectoryMetricsHolder<M> singleThreaded() {
        return new SingleThreadDirectoryMetricHolder<>(this);
    }

    public M instance() {
        return threadLocal.get();
    }
}
