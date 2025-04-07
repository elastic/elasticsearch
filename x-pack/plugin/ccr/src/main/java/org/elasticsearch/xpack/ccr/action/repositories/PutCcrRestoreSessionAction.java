/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;

public class PutCcrRestoreSessionAction extends ActionType<PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse> {

    public static final PutCcrRestoreSessionAction INTERNAL_INSTANCE = new PutCcrRestoreSessionAction();
    public static final String INTERNAL_NAME = "internal:admin/ccr/restore/session/put";
    public static final String NAME = "indices:internal/admin/ccr/restore/session/put";
    public static final PutCcrRestoreSessionAction INSTANCE = new PutCcrRestoreSessionAction(NAME);
    public static final RemoteClusterActionType<PutCcrRestoreSessionResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        PutCcrRestoreSessionResponse::new
    );
    public static final RemoteClusterActionType<PutCcrRestoreSessionResponse> REMOTE_INTERNAL_TYPE = new RemoteClusterActionType<>(
        INTERNAL_NAME,
        PutCcrRestoreSessionResponse::new
    );

    private PutCcrRestoreSessionAction() {
        super(INTERNAL_NAME);
    }

    private PutCcrRestoreSessionAction(String name) {
        super(name);
    }

    abstract static class TransportPutCcrRestoreSessionAction extends TransportSingleShardAction<
        PutCcrRestoreSessionRequest,
        PutCcrRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        private TransportPutCcrRestoreSessionAction(
            String actionName,
            ThreadPool threadPool,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver resolver,
            TransportService transportService,
            IndicesService indicesService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(
                actionName,
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                projectResolver,
                resolver,
                PutCcrRestoreSessionRequest::new,
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected PutCcrRestoreSessionResponse shardOperation(PutCcrRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Store.MetadataSnapshot storeFileMetadata = ccrRestoreService.openSession(request.getSessionUUID(), indexShard);
            long mappingVersion = indexShard.indexSettings().getIndexMetadata().getMappingVersion();
            return new PutCcrRestoreSessionResponse(clusterService.localNode(), storeFileMetadata, mappingVersion);
        }

        @Override
        protected Writeable.Reader<PutCcrRestoreSessionResponse> getResponseReader() {
            return PutCcrRestoreSessionResponse::new;
        }

        @Override
        protected boolean resolveIndex(PutCcrRestoreSessionRequest request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ProjectState state, InternalRequest request) {
            final ShardId shardId = request.request().getShardId();
            return state.routingTable().shardRoutingTable(shardId).primaryShardIt();
        }
    }

    public static class InternalTransportAction extends TransportPutCcrRestoreSessionAction {
        @Inject
        public InternalTransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver resolver,
            TransportService transportService,
            IndicesService indicesService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(
                INTERNAL_NAME,
                threadPool,
                clusterService,
                actionFilters,
                projectResolver,
                resolver,
                transportService,
                indicesService,
                ccrRestoreService
            );
        }
    }

    public static class TransportAction extends TransportPutCcrRestoreSessionAction {
        @Inject
        public TransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver resolver,
            TransportService transportService,
            IndicesService indicesService,
            CcrRestoreSourceService ccrRestoreService
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                actionFilters,
                projectResolver,
                resolver,
                transportService,
                indicesService,
                ccrRestoreService
            );
        }
    }

    public static class PutCcrRestoreSessionResponse extends ActionResponse {

        private final DiscoveryNode node;
        private final Store.MetadataSnapshot storeFileMetadata;
        private final long mappingVersion;

        PutCcrRestoreSessionResponse(DiscoveryNode node, Store.MetadataSnapshot storeFileMetadata, long mappingVersion) {
            this.node = node;
            this.storeFileMetadata = storeFileMetadata;
            this.mappingVersion = mappingVersion;
        }

        PutCcrRestoreSessionResponse(StreamInput in) throws IOException {
            node = new DiscoveryNode(in);
            storeFileMetadata = Store.MetadataSnapshot.readFrom(in);
            mappingVersion = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            node.writeTo(out);
            storeFileMetadata.writeTo(out);
            out.writeVLong(mappingVersion);
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public Store.MetadataSnapshot getStoreFileMetadata() {
            return storeFileMetadata;
        }

        public long getMappingVersion() {
            return mappingVersion;
        }
    }
}
