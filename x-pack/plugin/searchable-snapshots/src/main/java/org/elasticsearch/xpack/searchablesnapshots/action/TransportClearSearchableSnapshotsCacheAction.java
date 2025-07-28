/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction.EmptyResult;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;

public class TransportClearSearchableSnapshotsCacheAction extends AbstractTransportSearchableSnapshotsAction<
    ClearSearchableSnapshotsCacheRequest,
    BroadcastResponse,
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
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            indicesService,
            licenseState,
            false
        );
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) {
        return EmptyResult.INSTANCE;
    }

    @Override
    protected ResponseFactory<BroadcastResponse, EmptyResult> getResponseFactory(
        ClearSearchableSnapshotsCacheRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, emptyResults, shardFailures) -> new BroadcastResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
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
        directory.clearCache(false, true);
        return EmptyResult.INSTANCE;
    }
}
