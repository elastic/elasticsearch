/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class DeleteCcrRestoreSessionAction extends Action<DeleteCcrRestoreSessionAction.DeleteCcrRestoreSessionResponse> {

    public static final DeleteCcrRestoreSessionAction INSTANCE = new DeleteCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/delete";

    protected DeleteCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public DeleteCcrRestoreSessionResponse newResponse() {
        return new DeleteCcrRestoreSessionResponse();
    }

    public static class TransportDeleteCcrRestoreSessionAction
        extends TransportSingleShardAction<DeleteCcrRestoreSessionRequest, DeleteCcrRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(ThreadPool threadPool, ClusterService clusterService, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver resolver, TransportService transportService,
                                                      IndicesService indicesService, CcrRestoreSourceService ccrRestoreService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, resolver, DeleteCcrRestoreSessionRequest::new,
                ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected DeleteCcrRestoreSessionResponse shardOperation(DeleteCcrRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            ccrRestoreService.closeSession(request.getSessionUUID(), indexShard);
            return new DeleteCcrRestoreSessionResponse();
        }

        @Override
        protected DeleteCcrRestoreSessionResponse newResponse() {
            return new DeleteCcrRestoreSessionResponse();
        }

        @Override
        protected boolean resolveIndex(DeleteCcrRestoreSessionRequest request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            final ShardId shardId = request.request().getShardId();
            // The index uuid is not correct if we restore with a rename
            IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shardId.getIndexName(), shardId.id());
            return shardRoutingTable.primaryShardIt();
        }
    }

    public static class DeleteCcrRestoreSessionResponse extends ActionResponse {
    }
}
