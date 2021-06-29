/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;

public interface Renormalizer {

    /**
     * Is renormalization enabled?
     * @return {@code true} if renormalization is enabled or {@code false} otherwise
     */
    boolean isEnabled();

    /**
     * Update the anomaly score field on all previously persisted buckets
     * and all contained records
     */
    void renormalize(Quantiles quantiles);

    /**
     * Blocks until the renormalizer is idle and no further quantiles updates are pending.
     */
    void waitUntilIdle();

    /**
     * Shut down the renormalization ASAP.  Do not wait for it to fully complete.
     */
    void shutdown();
}
