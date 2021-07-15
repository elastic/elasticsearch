/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Map;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.isSearchableSnapshotStore;

public class SearchableSnapshotsFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    @Inject
    public SearchableSnapshotsFeatureSet(@Nullable XPackLicenseState licenseState, ClusterService clusterService) {
        this.licenseState = licenseState;
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.SEARCHABLE_SNAPSHOTS;
    }

    @Override
    public boolean available() {
        return licenseState.isAllowed(XPackLicenseState.Feature.SEARCHABLE_SNAPSHOTS);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        ClusterState state = clusterService.state();
        int numFullCopySnapIndices = 0;
        int numSharedCacheSnapIndices = 0;
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (isSearchableSnapshotStore(indexMetadata.getSettings())) {
                if (SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING.get(indexMetadata.getSettings())) {
                    numSharedCacheSnapIndices++;
                } else {
                    numFullCopySnapIndices++;
                }
            }
        }
        listener.onResponse(
            new SearchableSnapshotFeatureSetUsage(
                available(),
                numFullCopySnapIndices,
                numSharedCacheSnapIndices
            )
        );
    }
}
