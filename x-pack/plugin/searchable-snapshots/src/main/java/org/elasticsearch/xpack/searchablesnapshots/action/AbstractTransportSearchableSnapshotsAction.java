/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory.unwrapDirectory;

public abstract class AbstractTransportSearchableSnapshotsAction<
    Request extends BroadcastRequest<Request>,
    Response extends BaseBroadcastResponse,
    ShardOperationResult extends Writeable> extends TransportBroadcastByNodeAction<Request, Response, ShardOperationResult> {

    private final IndicesService indicesService;
    private final XPackLicenseState licenseState;

    AbstractTransportSearchableSnapshotsAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver resolver,
        Writeable.Reader<Request> request,
        String executor,
        IndicesService indicesService,
        XPackLicenseState licenseState
    ) {
        super(actionName, clusterService, transportService, actionFilters, resolver, request, executor);
        this.indicesService = indicesService;
        this.licenseState = Objects.requireNonNull(licenseState);
    }

    AbstractTransportSearchableSnapshotsAction(
        String actionName,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver resolver,
        Writeable.Reader<Request> request,
        String executor,
        IndicesService indicesService,
        XPackLicenseState licenseState,
        boolean canTripCircuitBreaker
    ) {
        super(actionName, clusterService, transportService, actionFilters, resolver, request, executor, canTripCircuitBreaker);
        this.indicesService = indicesService;
        this.licenseState = Objects.requireNonNull(licenseState);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] indices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indices);
    }

    @Override
    protected ShardsIterator shards(ClusterState state, Request request, String[] concreteIndices) {
        final List<String> searchableSnapshotIndices = new ArrayList<>();
        for (String concreteIndex : concreteIndices) {
            IndexMetadata indexMetaData = state.metadata().index(concreteIndex);
            if (indexMetaData != null) {
                if (indexMetaData.isSearchableSnapshot()) {
                    searchableSnapshotIndices.add(concreteIndex);
                }
            }
        }
        if (searchableSnapshotIndices.isEmpty()) {
            throw new ResourceNotFoundException("No searchable snapshots indices found");
        }
        return state.routingTable().allShards(searchableSnapshotIndices.toArray(new String[0]));
    }

    @Override
    protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<ShardOperationResult> listener) {
        ActionListener.completeWith(listener, () -> {
            SearchableSnapshots.ensureValidLicense(licenseState);
            final IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.index()).getShard(shardRouting.id());
            final SearchableSnapshotDirectory directory = unwrapDirectory(indexShard.store().directory());
            assert directory != null;
            assert directory.getShardId().equals(shardRouting.shardId());
            return executeShardOperation(request, shardRouting, directory);
        });
    }

    protected abstract ShardOperationResult executeShardOperation(
        Request request,
        ShardRouting shardRouting,
        SearchableSnapshotDirectory directory
    ) throws IOException;
}
