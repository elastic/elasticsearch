/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.regions.Region;

import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.function.Supplier;

/**
 * Holds onto the {@link Region} provided by the given supplier (think: the AWS SDK's default provider chain) in case it's needed for a S3
 * repository. If the supplier fails with an exception, the first call to {@link #getDefaultRegion} will log a warning message recording
 * the exception.
 */
class S3DefaultRegionHolder {

    private static final Logger logger = LogManager.getLogger(S3DefaultRegionHolder.class);

    // no synchronization required, assignments happen in start() which happens-before all reads
    private Region defaultRegion;
    private Runnable defaultRegionFailureLogger = () -> {};

    private final Runnable initializer;

    /**
     * @param delegateRegionSupplier Supplies a non-null {@link Region} or throws a {@link RuntimeException}.
     *                               <p>
     *                               Retained until its first-and-only invocation when {@link #start()} is called, and then released.
     */
    S3DefaultRegionHolder(Supplier<Region> delegateRegionSupplier) {
        initializer = new RunOnce(() -> {
            try {
                defaultRegion = delegateRegionSupplier.get();
                assert defaultRegion != null;
            } catch (Exception e) {
                defaultRegion = null;
                defaultRegionFailureLogger = new RunOnce(() -> logger.warn("failed to obtain region from default provider chain", e));
            }
        });
    }

    void start() {
        initializer.run();
    }

    Region getDefaultRegion() {
        assert defaultRegion != null || defaultRegionFailureLogger instanceof RunOnce : "not initialized";
        defaultRegionFailureLogger.run();
        return defaultRegion;
    }
}
