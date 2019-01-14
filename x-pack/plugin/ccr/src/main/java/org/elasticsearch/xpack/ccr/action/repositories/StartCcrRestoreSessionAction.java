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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class StartCcrRestoreSessionAction extends Action<StartCcrRestoreSessionAction.PutCcrRestoreSessionResponse> {

    public static final StartCcrRestoreSessionAction INSTANCE = new StartCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/put";

    private StartCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public PutCcrRestoreSessionResponse newResponse() {
        return new PutCcrRestoreSessionResponse();
    }

    @Override
    public Writeable.Reader<StartCcrRestoreSessionAction.PutCcrRestoreSessionResponse> getResponseReader() {
        return StartCcrRestoreSessionAction.PutCcrRestoreSessionResponse::new;
    }

    public static class TransportPutCcrRestoreSessionAction
        extends TransportSingleShardAction<StartCcrRestoreSessionRequest, PutCcrRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportPutCcrRestoreSessionAction(ThreadPool threadPool, ClusterService clusterService, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver resolver, TransportService transportService,
                                                   IndicesService indicesService, CcrRestoreSourceService ccrRestoreService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, resolver, StartCcrRestoreSessionRequest::new,
                ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected PutCcrRestoreSessionResponse shardOperation(StartCcrRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Tuple<String, Store.MetadataSnapshot> session = ccrRestoreService.openSession(indexShard);
            return new PutCcrRestoreSessionResponse(clusterService.localNode(), session.v1(), session.v2());
        }

        @Override
        protected PutCcrRestoreSessionResponse newResponse() {
            return new PutCcrRestoreSessionResponse();
        }

        @Override
        protected boolean resolveIndex(StartCcrRestoreSessionRequest request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            final ShardId shardId = request.request().getShardId();
            return state.routingTable().shardRoutingTable(shardId).primaryShardIt();
        }
    }


    public static class PutCcrRestoreSessionResponse extends ActionResponse {

        private DiscoveryNode node;
        private String sessionUUID;
        private Store.MetadataSnapshot storeFileMetaData;

        PutCcrRestoreSessionResponse() {
        }

        PutCcrRestoreSessionResponse(DiscoveryNode node, String sessionUUID, Store.MetadataSnapshot storeFileMetaData) {
            this.node = node;
            this.sessionUUID = sessionUUID;
            this.storeFileMetaData = storeFileMetaData;
        }

        PutCcrRestoreSessionResponse(StreamInput in) throws IOException {
            super(in);
            node = new DiscoveryNode(in);
            sessionUUID = in.readString();
            storeFileMetaData = new Store.MetadataSnapshot(in);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
            sessionUUID = in.readString();
            storeFileMetaData = new Store.MetadataSnapshot(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
            out.writeString(sessionUUID);
            storeFileMetaData.writeTo(out);
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public String getSessionUUID() {
            return sessionUUID;
        }

        public Store.MetadataSnapshot getStoreFileMetaData() {
            return storeFileMetaData;
        }
    }
}
