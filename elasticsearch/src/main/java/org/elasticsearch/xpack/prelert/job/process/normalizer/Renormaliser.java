/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.normalizer;

import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;

public interface Renormaliser {
    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records
     */
    void renormalise(Quantiles quantiles);

    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records and aggregate records to the partition
     * level
     */
    void renormaliseWithPartition(Quantiles quantiles);


    /**
     * Blocks until the renormaliser is idle and no further normalisation tasks are pending.
     */
    void waitUntilIdle();

    /**
     * Shut down the renormaliser
     */
    boolean shutdown();
}
