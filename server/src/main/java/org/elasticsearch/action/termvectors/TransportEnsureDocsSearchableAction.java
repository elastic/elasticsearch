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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
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

/**
 * This action is used in serverless to ensure that documents are searchable on the search tier before processing
 * term vector requests. It is an intermediate action that is executed on the indexing node and responds
 * with a no-op (the search node can proceed to process the term vector request). The action may trigger an external refresh
 * to ensure the search shards are up to date before returning the no-op.
 */
public class TransportEnsureDocsSearchableAction extends TransportSingleShardAction<
    TransportEnsureDocsSearchableAction.EnsureDocsSearchableRequest,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportEnsureDocsSearchableAction.class);
    private final NodeClient client;
    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "/eds";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(ACTION_NAME);

    @Inject
    public TransportEnsureDocsSearchableAction(
        ClusterService clusterService,
        NodeClient client,
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
            TransportEnsureDocsSearchableAction.EnsureDocsSearchableRequest::new,
            threadPool.executor(ThreadPool.Names.GET)
        );
        this.client = client;
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<ActionResponse.Empty> getResponseReader() {
        return in -> ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected boolean resolveIndex(TransportEnsureDocsSearchableAction.EnsureDocsSearchableRequest request) {
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
        TransportEnsureDocsSearchableAction.EnsureDocsSearchableRequest request,
        ShardId shardId,
        ActionListener<ActionResponse.Empty> listener
    ) throws IOException {
        assert DiscoveryNode.isStateless(clusterService.getSettings()) : ACTION_NAME + " should only be used in stateless";
        assert DiscoveryNode.hasRole(clusterService.getSettings(), DiscoveryNodeRole.INDEX_ROLE)
            : ACTION_NAME + " should only be executed on a stateless indexing node";
        logger.debug("received request with {} docs", request.docIds.length);
        getExecutor(shardId).execute(() -> ActionListener.run(listener, l -> {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            boolean docsFoundInLiveVersionMap = false;
            for (String docId : request.docIds()) {
                final var docUid = Uid.encodeId(docId);
                // There are a couple of limited cases where we may unnecessarily trigger an additional external refresh:
                // 1. Asking whether a document is in the live version map may incur a stateless refresh in itself.
                // 2. The document may be in the live version map archive, even though it has been refreshed to the search shards. The
                // document will be removed from the archive in a subsequent stateless refresh.
                // We prefer simplicity to complexity (trying to avoid the unnecessary stateless refresh) for the above limited cases.
                boolean docInLiveVersionMap = indexShard.withEngine(engine -> engine.isDocumentInLiveVersionMap(docUid));
                if (docInLiveVersionMap) {
                    logger.debug("doc id [{}] (uid [{}]) found in live version map of index shard [{}]", docId, docUid, shardId);
                    docsFoundInLiveVersionMap = true;
                    break;
                }
            }

            if (docsFoundInLiveVersionMap) {
                logger.debug("refreshing index shard [{}] due to mtv_eds", shardId);
                BasicReplicationRequest refreshRequest = new BasicReplicationRequest(shardId);
                refreshRequest.waitForActiveShards(ActiveShardCount.NONE);
                client.executeLocally(TransportShardRefreshAction.TYPE, refreshRequest, l.delegateFailureAndWrap((ll, r) -> {
                    // TransportShardRefreshAction.UnpromotableReplicasRefreshProxy.onPrimaryOperationComplete() returns a
                    // single shard failure if unpromotable(s) failed, with a combined list of (supressed) exceptions.
                    if (r.getShardInfo().getFailed() > 0) {
                        assert r.getShardInfo().getFailed() == 1
                            : "expected a single shard failure, got " + r.getShardInfo().getFailed() + " failures";
                        throw new ElasticsearchException("failed to refresh [{}]", r.getShardInfo().getFailures()[0].getCause(), shardId);
                    }
                    logger.debug("refreshed index shard [{}] due to mtv_eds", shardId);
                    ll.onResponse(ActionResponse.Empty.INSTANCE);
                }));
            } else {
                // Notice that there cannot be a race between the document(s) being evicted from the live version map due to an
                // ongoing refresh and before the search shards being updated with the new commit, because the documents are
                // guaranteed to be the in the live version map archive until search shards are updated with the new commit.
                // Thus, we can safely respond immediately as a no-op.
                logger.debug("mts_eds does not require refresh of index shard [{}]", shardId);
                l.onResponse(ActionResponse.Empty.INSTANCE);
            }
        }));
    }

    @Override
    protected ActionResponse.Empty shardOperation(
        TransportEnsureDocsSearchableAction.EnsureDocsSearchableRequest request,
        ShardId shardId
    ) {
        throw new UnsupportedOperationException();
    }

    public static final class EnsureDocsSearchableRequest extends SingleShardRequest<EnsureDocsSearchableRequest> {

        private int shardId; // this is not transmitted over the wire
        private String[] docIds;

        public EnsureDocsSearchableRequest() {}

        EnsureDocsSearchableRequest(StreamInput in) throws IOException {
            super(in);
            docIds = in.readStringArray();
        }

        @Override
        public ActionRequestValidationException validate() {
            return super.validateNonNullIndex();
        }

        public EnsureDocsSearchableRequest(String index, int shardId, String[] docIds) {
            super(index);
            this.shardId = shardId;
            this.docIds = docIds;
        }

        public int shardId() {
            return this.shardId;
        }

        public String[] docIds() {
            return docIds;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(docIds);
        }
    }

}
