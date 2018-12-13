/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;
import java.util.List;

public class DeleteCcrRestoreSessionAction extends Action<DeleteCcrRestoreSessionAction.DeleteCcrRestoreSessionResponse> {

    public static final DeleteCcrRestoreSessionAction INSTANCE = new DeleteCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/delete";

    private DeleteCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public DeleteCcrRestoreSessionResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    public static class TransportDeleteCcrRestoreSessionAction extends TransportNodesAction<DeleteCcrRestoreSessionRequest,
        DeleteCcrRestoreSessionResponse, DeleteCcrRestoreSessionRequest.DeleteRequest, DeleteResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportDeleteCcrRestoreSessionAction(ThreadPool threadPool, ClusterService clusterService, ActionFilters actionFilters,
                                                      TransportService transportService, IndicesService indicesService,
                                                      CcrRestoreSourceService ccrRestoreService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, DeleteCcrRestoreSessionRequest::new,
                DeleteCcrRestoreSessionRequest.DeleteRequest::new, ThreadPool.Names.GENERIC, DeleteResponse.class);
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected DeleteCcrRestoreSessionResponse newResponse(DeleteCcrRestoreSessionRequest request, List<DeleteResponse> deleteResponses,
                                                              List<FailedNodeException> failures) {
            return new DeleteCcrRestoreSessionResponse(clusterService.getClusterName(), deleteResponses, failures);
        }

        @Override
        protected DeleteCcrRestoreSessionRequest.DeleteRequest newNodeRequest(String nodeId, DeleteCcrRestoreSessionRequest request) {
            return null;
        }

        @Override
        protected DeleteResponse newNodeResponse() {
            return null;
        }

        @Override
        protected DeleteResponse nodeOperation(DeleteCcrRestoreSessionRequest.DeleteRequest request) {
            ShardId shardId = null;
            String sessionUUID = "";
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            ccrRestoreService.closeSession(sessionUUID, indexShard);
            return new DeleteResponse(clusterService.localNode());
        }
    }

    public static class DeleteResponse extends BaseNodeResponse {

        private DeleteResponse() {
        }

        private DeleteResponse(StreamInput streamInput) throws IOException {
            readFrom(streamInput);
        }

        private DeleteResponse(DiscoveryNode node) {
            super(node);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }
    }

    public static class DeleteCcrRestoreSessionResponse extends BaseNodesResponse<DeleteResponse> {

        DeleteCcrRestoreSessionResponse(StreamInput streamInput) throws IOException {
            readFrom(streamInput);
        }

        DeleteCcrRestoreSessionResponse(ClusterName clusterName, List<DeleteResponse> chunkResponses, List<FailedNodeException> failures) {
            super(clusterName, chunkResponses, failures);
        }

        @Override
        protected List<DeleteResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(DeleteResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<DeleteResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        public void getThing() {
            if (hasFailures()) {
                throw failures().get(0);
            }
        }
    }
}
