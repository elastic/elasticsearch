/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.Map;

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
        return XPackField.ENRICH;
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
        int numSnapIndices = 0;
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings())) {
                numSnapIndices++;
            }
        }
        listener.onResponse(
            new SearchableSnapshotFeatureSetUsage(
                available(),
                numSnapIndices
            )
        );
    }
}
