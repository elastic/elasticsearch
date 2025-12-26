/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

public class SingleThreadMetricHolder<M> implements MetricHolder<M> {
    private final ThreadLocalMetricHolder<M> delegate;
    private Thread owner;
    private M current;

    public SingleThreadMetricHolder(ThreadLocalMetricHolder<M> delegate) {
        this.delegate = delegate;
    }

    public SingleThreadMetricHolder(SingleThreadMetricHolder<M> source) {
        this.delegate = source.delegate;
        this.owner = source.owner;
        this.current = source.current;
    }

    @Override
    public MetricHolder<M> singleThreaded() {
        return new SingleThreadMetricHolder<>(this);
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
