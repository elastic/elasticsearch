/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.license.XPackLicenseState;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

final class SearchableSnapshotsUsageTracker implements Runnable {

    private final XPackLicenseState licenseState;
    private final Supplier<ClusterState> clusterStateSupplier;

    SearchableSnapshotsUsageTracker(XPackLicenseState licenseState, Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
        this.licenseState = licenseState;
    }

    @Override
    public void run() {
        if (hasSearchableSnapshotsIndices(clusterStateSupplier.get())) {
            SEARCHABLE_SNAPSHOT_FEATURE.check(licenseState);
        }
    }

    private static boolean hasSearchableSnapshotsIndices(ClusterState state) {
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (indexMetadata.isSearchableSnapshot()) {
                return true;
            }
        }
        return false;
    }
}
