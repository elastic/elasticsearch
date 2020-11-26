/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction.EmptyResult;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportClearSearchableSnapshotsCacheAction extends AbstractTransportSearchableSnapshotsAction<
    ClearSearchableSnapshotsCacheRequest,
    ClearSearchableSnapshotsCacheResponse,
    EmptyResult> {

    @Inject
    public TransportClearSearchableSnapshotsCacheAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        XPackLicenseState licenseState
    ) {
        super(
            ClearSearchableSnapshotsCacheAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ClearSearchableSnapshotsCacheRequest::new,
            ThreadPool.Names.MANAGEMENT,
            indicesService,
            licenseState,
            false
        );
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ClearSearchableSnapshotsCacheResponse newResponse(
        ClearSearchableSnapshotsCacheRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new ClearSearchableSnapshotsCacheResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ClearSearchableSnapshotsCacheRequest readRequestFrom(StreamInput in) throws IOException {
        return new ClearSearchableSnapshotsCacheRequest(in);
    }

    @Override
    protected EmptyResult executeShardOperation(
        ClearSearchableSnapshotsCacheRequest request,
        ShardRouting shardRouting,
        SearchableSnapshotDirectory directory
    ) {
        directory.clearCache();
        return EmptyResult.INSTANCE;
    }
}
