/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import java.util.concurrent.atomic.AtomicLong;

public class RollupBulkStats {
    private final AtomicLong totalBulkCount = new AtomicLong(0);
    private final AtomicLong bulkDurationSumMillis = new AtomicLong(0);
    private final AtomicLong maxBulkDurationMillis = new AtomicLong(-1);
    private final AtomicLong minBulkDurationMillis = new AtomicLong(-1);
    private final AtomicLong bulkIngestSumMillis = new AtomicLong(0);
    private final AtomicLong maxBulkIngestMillis = new AtomicLong(-1);
    private final AtomicLong minBulkIngestMillis = new AtomicLong(-1);
    private final AtomicLong bulkTookSumMillis = new AtomicLong(0);
    private final AtomicLong maxBulkTookMillis = new AtomicLong(-1);
    private final AtomicLong minBulkTookMillis = new AtomicLong(-1);

    public void update(long bulkDurationMillis, long bulkIngestMillis, long bulkTookMillis) {
        this.totalBulkCount.incrementAndGet();

        this.bulkDurationSumMillis.addAndGet(bulkDurationMillis);
        this.maxBulkDurationMillis.updateAndGet(existingValue -> max(bulkDurationMillis, existingValue));
        this.minBulkDurationMillis.updateAndGet(existingValue -> min(bulkDurationMillis, existingValue));

        this.bulkIngestSumMillis.addAndGet(bulkIngestMillis);
        this.maxBulkIngestMillis.updateAndGet(existingValue -> max(bulkIngestMillis, existingValue));
        this.minBulkIngestMillis.updateAndGet(existingValue -> min(bulkIngestMillis, existingValue));

        this.bulkTookSumMillis.addAndGet(bulkTookMillis);
        this.maxBulkTookMillis.updateAndGet(existingValue -> max(bulkTookMillis, existingValue));
        this.minBulkTookMillis.updateAndGet(existingValue -> min(bulkTookMillis, existingValue));
    }

    private static long min(long newValue, long existingValue) {
        return existingValue == -1 ? newValue : Math.min(newValue, existingValue);
    }

    private static long max(long newValue, long existingValue) {
        return existingValue == -1 ? newValue : Math.max(newValue, existingValue);
    }

    /**
     * @return An instance of {@link RollupBulkInfo} including rollup bulk indexing statistics.
     */
    public RollupBulkInfo getRollupBulkInfo() {
        return new RollupBulkInfo(
            this.totalBulkCount.get(),
            this.bulkDurationSumMillis.get(),
            this.maxBulkDurationMillis.get(),
            this.minBulkDurationMillis.get(),
            this.bulkIngestSumMillis.get(),
            this.maxBulkIngestMillis.get(),
            this.minBulkIngestMillis.get(),
            this.bulkTookSumMillis.get(),
            this.maxBulkTookMillis.get(),
            this.minBulkTookMillis.get()
        );
    }
}
