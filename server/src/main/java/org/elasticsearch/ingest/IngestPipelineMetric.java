/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.metrics.CounterMetric;

/**
 * <p>Metrics to measure ingest actions, specific to pipelines.
 */
public class IngestPipelineMetric extends IngestMetric {

    /**
     * The amount of bytes ingested by a pipeline.
     */
    private final CounterMetric bytesIngested = new CounterMetric();

    /**
     * The amount of bytes produced by a pipeline.
     */
    private final CounterMetric bytesProduced = new CounterMetric();

    void add(IngestPipelineMetric metrics) {
        super.add(metrics);
        bytesIngested.inc(metrics.bytesIngested.count());
        bytesProduced.inc(metrics.bytesProduced.count());
    }

    /**
     * Call this prior to the ingest action.
     * @param bytesIngested The number of bytes ingested by the pipeline.
     */
    void preIngestBytes(long bytesIngested) {
        this.bytesIngested.inc(bytesIngested);
    }

    /**
     * Call this after performing the ingest action.
     * @param bytesProduced The number of bytes resulting from running a request in the pipeline.
     */
    void postIngestBytes(long bytesProduced) {
        this.bytesProduced.inc(bytesProduced);
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    IngestStats.ByteStats createByteStats() {
        long bytesIngested = this.bytesIngested.count();
        long bytesProduced = this.bytesProduced.count();
        if (bytesIngested == 0L && bytesProduced == 0L) {
            return IngestStats.ByteStats.IDENTITY;
        }
        return new IngestStats.ByteStats(bytesIngested, bytesProduced);
    }

}
