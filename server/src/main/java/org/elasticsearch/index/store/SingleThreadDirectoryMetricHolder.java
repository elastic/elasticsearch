/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

public class SingleThreadDirectoryMetricHolder<M extends DirectoryMetrics.PluggableMetrics<M>>
    implements
        PluggableDirectoryMetricsHolder<M> {
    private final ThreadLocalDirectoryMetricHolder<M> delegate;
    private Thread owner;
    private M current;

    public SingleThreadDirectoryMetricHolder(ThreadLocalDirectoryMetricHolder<M> delegate) {
        this.delegate = delegate;
    }

    public SingleThreadDirectoryMetricHolder(SingleThreadDirectoryMetricHolder<M> source) {
        this.delegate = source.delegate;
        this.owner = source.owner;
        this.current = source.current;
    }

    @Override
    public PluggableDirectoryMetricsHolder<M> singleThreaded() {
        return new SingleThreadDirectoryMetricHolder<>(this);
    }

    public M instance() {
        Thread thread = Thread.currentThread();
        if (owner == thread) {
            return current;
        } else {
            current = delegate.instance();
            owner = thread;
            return current;
        }
    }
}
