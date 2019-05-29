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
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class PutCcrRestoreSessionAction extends Action<PutCcrRestoreSessionRequest,
    PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse, PutCcrRestoreSessionRequestBuilder> {

    public static final PutCcrRestoreSessionAction INSTANCE = new PutCcrRestoreSessionAction();
    public static final String NAME = "internal:admin/ccr/restore/session/put";

    private PutCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public PutCcrRestoreSessionResponse newResponse() {
        return new PutCcrRestoreSessionResponse();
    }

    @Override
    public Writeable.Reader<PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse> getResponseReader() {
        return PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse::new;
    }

    @Override
    public PutCcrRestoreSessionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PutCcrRestoreSessionRequestBuilder(client);
    }

    public static class TransportPutCcrRestoreSessionAction
        extends TransportSingleShardAction<PutCcrRestoreSessionRequest, PutCcrRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportPutCcrRestoreSessionAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                   ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                   TransportService transportService, IndicesService indicesService,
                                                   CcrRestoreSourceService ccrRestoreService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters, resolver,
                PutCcrRestoreSessionRequest::new, ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected PutCcrRestoreSessionResponse shardOperation(PutCcrRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Store.MetadataSnapshot storeFileMetaData = ccrRestoreService.openSession(request.getSessionUUID(), indexShard);
            long mappingVersion = indexShard.indexSettings().getIndexMetaData().getMappingVersion();
            return new PutCcrRestoreSessionResponse(clusterService.localNode(), storeFileMetaData, mappingVersion);
        }

        @Override
        protected PutCcrRestoreSessionResponse newResponse() {
            return new PutCcrRestoreSessionResponse();
        }

        @Override
        protected boolean resolveIndex(PutCcrRestoreSessionRequest request) {
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
        private Store.MetadataSnapshot storeFileMetaData;
        private long mappingVersion;

        PutCcrRestoreSessionResponse() {
        }

        PutCcrRestoreSessionResponse(DiscoveryNode node, Store.MetadataSnapshot storeFileMetaData, long mappingVersion) {
            this.node = node;
            this.storeFileMetaData = storeFileMetaData;
            this.mappingVersion = mappingVersion;
        }

        PutCcrRestoreSessionResponse(StreamInput in) throws IOException {
            super(in);
            node = new DiscoveryNode(in);
            storeFileMetaData = new Store.MetadataSnapshot(in);
            mappingVersion = in.readVLong();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
            storeFileMetaData = new Store.MetadataSnapshot(in);
            mappingVersion = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
            storeFileMetaData.writeTo(out);
            out.writeVLong(mappingVersion);
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public Store.MetadataSnapshot getStoreFileMetaData() {
            return storeFileMetaData;
        }

        public long getMappingVersion() {
            return mappingVersion;
        }
    }
}
