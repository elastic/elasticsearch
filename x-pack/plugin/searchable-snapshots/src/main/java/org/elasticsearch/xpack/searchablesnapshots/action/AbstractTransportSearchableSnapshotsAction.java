/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.InMemoryNoOpCommitDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_CACHE_ENABLED_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY;

public abstract class AbstractTransportSearchableSnapshotsAction
    <Request extends BroadcastRequest<Request>, Response extends BroadcastResponse, ShardOperationResult extends Writeable>
    extends TransportBroadcastByNodeAction<Request, Response, ShardOperationResult> {

    private final IndicesService indicesService;

    AbstractTransportSearchableSnapshotsAction(String actionName, ClusterService clusterService, TransportService transportService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                               Writeable.Reader<Request> request, String executor, IndicesService indicesService) {
        super(actionName, clusterService, transportService, actionFilters, resolver, request, executor);
        this.indicesService = indicesService;
    }

    AbstractTransportSearchableSnapshotsAction(String actionName, ClusterService clusterService, TransportService transportService,
                                               ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                               Writeable.Reader<Request> request, String executor, IndicesService indicesService,
                                               boolean canTripCircuitBreaker) {
        super(actionName, clusterService, transportService, actionFilters, resolver, request, executor, canTripCircuitBreaker);
        this.indicesService = indicesService;
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
            IndexMetaData indexMetaData = state.metaData().index(concreteIndex);
            if (indexMetaData != null) {
                Settings indexSettings = indexMetaData.getSettings();
                if (INDEX_STORE_TYPE_SETTING.get(indexSettings).equals(SNAPSHOT_DIRECTORY_FACTORY_KEY)) {
                    if (SNAPSHOT_CACHE_ENABLED_SETTING.get(indexSettings)) {
                        searchableSnapshotIndices.add(concreteIndex);
                    }
                }
            }
        }
        if (searchableSnapshotIndices.isEmpty()) {
            throw new ResourceNotFoundException("No searchable snapshots indices found");
        }
        return state.routingTable().allShards(searchableSnapshotIndices.toArray(new String[0]));
    }

    @Override
    protected ShardOperationResult shardOperation(Request request, ShardRouting shardRouting) throws IOException {
        final IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.index()).getShard(shardRouting.id());
        final CacheDirectory cacheDirectory = unwrapCacheDirectory(indexShard.store().directory());
        assert cacheDirectory != null;
        assert cacheDirectory.getShardId().equals(shardRouting.shardId());
        return executeShardOperation(request, shardRouting, cacheDirectory);
    }

    protected abstract ShardOperationResult executeShardOperation(Request request, ShardRouting shardRouting,
                                                                  CacheDirectory cacheDirectory) throws IOException;

    @Nullable
    private static CacheDirectory unwrapCacheDirectory(Directory dir) {
        while (dir != null) {
            if (dir instanceof CacheDirectory) {
                return (CacheDirectory) dir;
            } else if (dir instanceof InMemoryNoOpCommitDirectory) {
                dir = ((InMemoryNoOpCommitDirectory) dir).getRealDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        return null;
    }
}
