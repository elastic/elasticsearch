/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local.distribution;

import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A {@link DistributionResolver} for resolving snapshot versions of Elasticsearch for previous, backwards-compatible versions.
 */
public class SnapshotDistributionResolver implements DistributionResolver {
    private static final String BWC_DISTRIBUTION_SYSPROP_PREFIX = "tests.snapshot.distribution.";
    private final DistributionResolver delegate;

    public SnapshotDistributionResolver(DistributionResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public DistributionDescriptor resolve(Version version, DistributionType type) {
        String distributionPath = System.getProperty(BWC_DISTRIBUTION_SYSPROP_PREFIX + version.toString());

        if (distributionPath != null) {
            Path distributionDir = Path.of(distributionPath);
            if (Files.notExists(distributionDir)) {
                throw new IllegalStateException(
                    "Cannot locate Elasticsearch distribution. Directory at '" + distributionDir + "' does not exist."
                );
            }

            // Snapshot distributions are never release builds and always use the default distribution
            Version realVersion = Version.fromString(System.getProperty("tests.bwc.main.version", version.toString()));
            boolean isSnapshot = System.getProperty("tests.bwc.snapshot", "true").equals("false") == false;
            return new DefaultDistributionDescriptor(realVersion, isSnapshot, distributionDir, DistributionType.DEFAULT);
        }

        return delegate.resolve(version, type);
    }
}
