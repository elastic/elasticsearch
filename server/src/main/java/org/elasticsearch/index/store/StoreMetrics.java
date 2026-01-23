/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

public class StoreMetrics {
    public static final MetricHolder<StoreMetrics> NOOP_HOLDER = MetricHolder.noop(new StoreMetrics() {
        @Override
        public void addBytesRead(long amount) {}
    });

    private long bytesRead;

    public long getBytesRead() {
        return bytesRead;
    }

    public StoreMetrics snapshot() {
        StoreMetrics metrics = new StoreMetrics();
        metrics.bytesRead = bytesRead;
        return metrics;
    }

    public void addBytesRead(long amount) {
        bytesRead += amount;
    }
}
