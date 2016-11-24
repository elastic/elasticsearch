/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.normalizer.noop;

import org.elasticsearch.xpack.prelert.job.process.normalizer.Renormaliser;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;

/**
 * A {@link Renormaliser} implementation that does absolutely nothing
 * This should be removed when the normaliser code is ported
 */
public class NoOpRenormaliser implements Renormaliser {
    // NORELEASE Remove once the normaliser code is ported
    @Override
    public void renormalise(Quantiles quantiles) {

    }

    @Override
    public void renormaliseWithPartition(Quantiles quantiles) {

    }

    @Override
    public void waitUntilIdle() {

    }

    @Override
    public boolean shutdown() {
        return true;
    }
}
