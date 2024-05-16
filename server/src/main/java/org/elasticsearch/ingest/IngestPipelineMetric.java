/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.metrics.CounterMetric;

/**
 * <p>Metrics to measure ingest actions, specific to pipelines.
 */
public class IngestPipelineMetric extends IngestMetric {

    /**
     * The amount of bytes received by a pipeline.
     */
    private final CounterMetric bytesReceived = new CounterMetric();

    /**
     * The amount of bytes produced by a pipeline.
     */
    private final CounterMetric bytesProduced = new CounterMetric();

    void add(IngestPipelineMetric metrics) {
        super.add(metrics);
        bytesReceived.inc(metrics.bytesReceived.count());
        bytesProduced.inc(metrics.bytesProduced.count());
    }

    /**
     * Call this prior to the ingest action.
     * @param bytesReceived The number of bytes received by the pipeline.
     */
    void preIngestBytes(long bytesReceived) {
        this.bytesReceived.inc(bytesReceived);
    }

    /**
     * Call this after performing the ingest action, even if the action failed.
     * @param bytesProduced The number of bytes resulting from running a request in the pipeline.
     */
    void postIngestBytes(long bytesProduced) {
        this.bytesProduced.inc(bytesProduced);
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    IngestStats.ByteStats createByteStats() {
        return new IngestStats.ByteStats(this.bytesReceived.count(), this.bytesProduced.count());
    }

}
