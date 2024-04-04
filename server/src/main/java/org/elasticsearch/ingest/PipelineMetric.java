/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import java.util.concurrent.atomic.LongAdder;

/**
 * <p>Metrics to measure ingest actions, specific to pipelines.
 * <p>Meant to be used in conjunction with IngestMetric
 */
public class PipelineMetric {

    /**
     * The amount of bytes received by a pipeline.
     */
    private final LongAdder bytesReceived = new LongAdder();

    /**
     * The amount of bytes produced by a pipeline.
     */
    private final LongAdder bytesProduced = new LongAdder();

    /**
     * Call this prior to the ingest action.
     * @param bytesReceived The number of bytes received by the pipeline.
     */
    void preIngest(long bytesReceived) {
        this.bytesReceived.add(bytesReceived);
    }

    /**
     * Call this after performing the ingest action, even if the action failed.
     * @param bytesProduced The number of bytes resulting from running a request in the pipeline.
     */
    void postIngest(long bytesProduced) {
        this.bytesProduced.add(bytesProduced);
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    IngestStats.ByteStats createByteStats() {
        return new IngestStats.ByteStats(this.bytesReceived.longValue(), this.bytesProduced.longValue());
    }

}
