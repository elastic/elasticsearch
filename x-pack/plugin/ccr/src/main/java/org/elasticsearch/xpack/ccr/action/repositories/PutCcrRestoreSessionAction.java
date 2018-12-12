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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutCcrRestoreSessionAction extends Action<PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse> {

    public static final PutCcrRestoreSessionAction INSTANCE = new PutCcrRestoreSessionAction();
    private static final String NAME = "internal:admin/ccr/restore/session/put";

    private PutCcrRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public PutCcrRestoreSessionResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse> getResponseReader() {
        return PutCcrRestoreSessionAction.PutCcrRestoreSessionResponse::new;
    }

    public static class TransportPutCcrRestoreSessionAction
        extends TransportSingleShardAction<PutCcrRestoreSessionRequest, PutCcrRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final CcrRestoreSourceService ccrRestoreService;

        @Inject
        public TransportPutCcrRestoreSessionAction(ThreadPool threadPool, ClusterService clusterService, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver resolver, TransportService transportService,
                                                   IndicesService indicesService, CcrRestoreSourceService ccrRestoreService) {
            super(NAME, threadPool, clusterService, transportService, actionFilters, resolver, PutCcrRestoreSessionRequest::new,
                ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
            this.ccrRestoreService = ccrRestoreService;
        }

        @Override
        protected PutCcrRestoreSessionResponse shardOperation(PutCcrRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Engine.IndexCommitRef commit = indexShard.acquireSafeIndexCommit();
            String sessionUUID = UUIDs.randomBase64UUID();
            ccrRestoreService.addCommit(sessionUUID, commit);
            final Store.MetadataSnapshot snapshot;
            indexShard.store().incRef();
            try {
                snapshot = indexShard.store().getMetadata(commit.getIndexCommit());
            } finally {
                indexShard.store().decRef();
            }
            return new PutCcrRestoreSessionResponse(indexShard.routingEntry().currentNodeId(), new ArrayList<>(), new ArrayList<>());
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
            // The index uuid is not correct if we restore with a rename
            IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shardId.getIndexName(), shardId.id());
            return shardRoutingTable.primaryShardIt();
        }
    }


    public static class PutCcrRestoreSessionResponse extends ActionResponse {

        private String nodeId;
        private List<StoreFileMetaData> identicalFiles;
        private List<StoreFileMetaData> filesToRecover;

        PutCcrRestoreSessionResponse() {
        }

        PutCcrRestoreSessionResponse(String nodeId, List<StoreFileMetaData> identicalFiles, List<StoreFileMetaData> filesToRecover) {
            this.nodeId = nodeId;
            this.identicalFiles = identicalFiles;
            this.filesToRecover = filesToRecover;
        }

        PutCcrRestoreSessionResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
            nodeId = streamInput.readString();
            identicalFiles = streamInput.readList(StoreFileMetaData::new);
            filesToRecover = streamInput.readList(StoreFileMetaData::new);
        }

        @Override
        public void readFrom(StreamInput streamInput) throws IOException {
            super.readFrom(streamInput);
            nodeId = streamInput.readString();
            identicalFiles = streamInput.readList(StoreFileMetaData::new);
            filesToRecover = streamInput.readList(StoreFileMetaData::new);
        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            streamOutput.writeString(nodeId);
            streamOutput.writeList(identicalFiles);
            streamOutput.writeList(filesToRecover);
        }

        public String getNodeId() {
            return nodeId;
        }

        public List<StoreFileMetaData> getIdenticalFiles() {
            return identicalFiles;
        }

        public List<StoreFileMetaData> getFilesToRecover() {
            return filesToRecover;
        }
    }
}
