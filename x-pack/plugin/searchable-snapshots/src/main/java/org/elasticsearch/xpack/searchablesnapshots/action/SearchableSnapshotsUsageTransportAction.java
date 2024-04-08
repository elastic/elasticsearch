/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotFeatureSetUsage;

import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

public class SearchableSnapshotsUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final XPackLicenseState licenseState;

    @Inject
    public SearchableSnapshotsUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            XPackUsageFeatureAction.SEARCHABLE_SNAPSHOTS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        int numFullCopySnapIndices = 0;
        int numSharedCacheSnapIndices = 0;
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (indexMetadata.isSearchableSnapshot()) {
                if (indexMetadata.isPartialSearchableSnapshot()) {
                    numSharedCacheSnapIndices++;
                } else {
                    numFullCopySnapIndices++;
                }
            }
        }
        listener.onResponse(
            new XPackUsageFeatureResponse(
                new SearchableSnapshotFeatureSetUsage(
                    SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState),
                    numFullCopySnapIndices,
                    numSharedCacheSnapIndices
                )
            )
        );
    }
}
