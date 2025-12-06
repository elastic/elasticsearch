/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.license.XPackLicenseState;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.lucene.bwc.OldLuceneVersions.ARCHIVE_FEATURE;

final class ArchiveUsageTracker implements Runnable {

    private final XPackLicenseState licenseState;
    private final Supplier<ClusterState> clusterStateSupplier;

    ArchiveUsageTracker(XPackLicenseState licenseState, Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.licenseState = licenseState;
    }

    @Override
    public void run() {
        if (hasArchiveIndices(clusterStateSupplier.get())) {
            ARCHIVE_FEATURE.check(licenseState);
        }
    }

    private static boolean hasArchiveIndices(ClusterState state) {
        for (IndexMetadata indexMetadata : state.metadata().getProject()) {
            if (indexMetadata.getCreationVersion().isLegacyIndexVersion()) {
                return true;
            }
        }
        return false;
    }
}
