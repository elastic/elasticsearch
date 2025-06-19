/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 *
 * This file was contributed to by generative AI
 */

package org.elasticsearch.action.termvectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportEnsureDocsSearchableAction extends TransportSingleShardAction<
    MultiTermVectorsShardRequest,
    TransportEnsureDocsSearchableAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportEnsureDocsSearchableAction.class);
    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "/eds";
    public static final ActionType<TransportEnsureDocsSearchableAction.Response> TYPE = new ActionType<>(ACTION_NAME);

    @Inject
    public TransportEnsureDocsSearchableAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            projectResolver,
            indexNameExpressionResolver,
            MultiTermVectorsShardRequest::new,
            threadPool.executor(ThreadPool.Names.GET)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<TransportEnsureDocsSearchableAction.Response> getResponseReader() {
        return TransportEnsureDocsSearchableAction.Response::new;
    }

    @Override
    protected boolean resolveIndex(MultiTermVectorsShardRequest request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ProjectState state, InternalRequest request) {
        assert DiscoveryNode.isStateless(clusterService.getSettings()) : ACTION_NAME + " should only be used in stateless";
        final var primaryShard = state.routingTable()
            .shardRoutingTable(request.concreteIndex(), request.request().shardId())
            .primaryShard();
        if (primaryShard.active() == false) {
            throw new NoShardAvailableActionException(primaryShard.shardId(), "primary shard is not active");
        }
        DiscoveryNode node = state.cluster().nodes().get(primaryShard.currentNodeId());
        assert node != null;
        return new ShardIterator(primaryShard.shardId(), List.of(primaryShard));
    }

    @Override
    protected void asyncShardOperation(
        MultiTermVectorsShardRequest request,
        ShardId shardId,
        ActionListener<TransportEnsureDocsSearchableAction.Response> listener
    ) throws IOException {
        assert DiscoveryNode.isStateless(clusterService.getSettings()) : ACTION_NAME + " should only be used in stateless";
        assert DiscoveryNode.hasRole(clusterService.getSettings(), DiscoveryNodeRole.INDEX_ROLE)
            : ACTION_NAME + " should only be executed on a stateless indexing node";
        logger.debug("received locally {} with {} sub requests", request, request.locations.size());
        getExecutor(shardId).execute(() -> ActionListener.run(listener, l -> {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            boolean refreshBeforeReturning = false;
            for (int i = 0; i < request.locations.size(); i++) {
                TermVectorsRequest termVectorsRequest = request.requests.get(i);
                String docId = termVectorsRequest.id();
                if (termVectorsRequest.realtime() && docId != null && docId.isEmpty() == false) {
                    final var docUid = Uid.encodeId(docId);
                    boolean docInLiveVersionMap = indexShard.withEngine(engine -> engine.isDocumentInLiveVersionMap(docUid));
                    if (docInLiveVersionMap) {
                        logger.debug("doc id [{}] (uid [{}]) requires refresh of index shard [{}]", docId, docUid, shardId);
                        refreshBeforeReturning = true;
                        break;
                    }
                }
            }
            long primaryTerm = indexShard.getOperationPrimaryTerm();
            if (refreshBeforeReturning) {
                logger.debug("refreshing index shard [{}] due to mtv_eds", shardId);
                indexShard.externalRefresh(
                    "refresh_mtv_eds",
                    l.map(r -> new TransportEnsureDocsSearchableAction.Response(primaryTerm, -1))
                );
            } else {
                // We respond with the current segment generation, because there is a race between the document(s) being evicted from the
                // live version map due to an ongoing refresh and before the search shards being updated with the new commit. Thus,
                // the search shard should ensure to wait for the segment generation before serving the term vector request locally.
                long segmentGeneration = indexShard.withEngine(engine -> engine.getLastCommittedSegmentInfos().getGeneration());
                final var response = new TransportEnsureDocsSearchableAction.Response(primaryTerm, segmentGeneration);
                logger.debug("mts_eds does not require refresh of index shard [{}], responding [{}]", shardId, response);
                l.onResponse(response);
            }
        }));
    }

    @Override
    protected TransportEnsureDocsSearchableAction.Response shardOperation(MultiTermVectorsShardRequest request, ShardId shardId) {
        throw new UnsupportedOperationException();
    }

    public static class Response extends ActionResponse {
        private final long primaryTerm;
        private final long segmentGeneration;

        public Response(long primaryTerm, long segmentGeneration) {
            this.primaryTerm = primaryTerm;
            this.segmentGeneration = segmentGeneration;
        }

        public Response(StreamInput in) throws IOException {
            primaryTerm = in.readVLong();
            segmentGeneration = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(primaryTerm);
            out.writeZLong(segmentGeneration);
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        /**
         * @return The segment generation that the search shard should wait for before handling the term vector API request locally.
         *         Returns -1 if the search shard does not need to wait for any segment generation.
         */
        public long segmentGeneration() {
            return segmentGeneration;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response response = (Response) o;
            return primaryTerm == response.primaryTerm && segmentGeneration == response.segmentGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryTerm, segmentGeneration);
        }

        @Override
        public String toString() {
            return Strings.format("Response{primaryTerm=%d, segmentGeneration=%d}", primaryTerm, segmentGeneration);
        }
    }
}
