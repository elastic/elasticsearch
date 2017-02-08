/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;

public interface Renormalizer {
    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records
     */
    void renormalize(Quantiles quantiles);

    /**
     * Blocks until the renormalizer is idle and no further quantiles updates are pending.
     */
    void waitUntilIdle();
}
