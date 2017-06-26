/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer.noop;

import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.process.normalizer.Renormalizer;

/**
 * A {@link Renormalizer} implementation that does absolutely nothing
 * This should be removed when the normalizer code is ported
 */
public class NoOpRenormalizer implements Renormalizer {

    @Override
    public void renormalize(Quantiles quantiles) {
    }

    @Override
    public void waitUntilIdle() {
    }

    @Override
    public void shutdown() {
    }
}
