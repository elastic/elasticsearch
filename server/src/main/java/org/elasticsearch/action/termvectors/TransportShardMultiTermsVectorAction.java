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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiTermsVectorAction extends TransportSingleShardAction<
    MultiTermVectorsShardRequest,
    MultiTermVectorsShardResponse> {

    private final NodeClient client;
    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "[shard]";
    public static final ActionType<MultiTermVectorsShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    @Inject
    public TransportShardMultiTermsVectorAction(
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
            MultiTermVectorsShardRequest::new,
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
    protected Writeable.Reader<MultiTermVectorsShardResponse> getResponseReader() {
        return MultiTermVectorsShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiTermVectorsShardRequest request) {
        return false;
    }

    @Override
    protected ShardIterator shards(ProjectState project, InternalRequest request) {
        ShardIterator iterator = clusterService.operationRouting()
            .getShards(project, request.concreteIndex(), request.request().shardId(), request.request().preference());
        if (iterator == null) {
            return null;
        }
        return ShardIterator.allSearchableShards(iterator);
    }

    @Override
    protected void asyncShardOperation(
        MultiTermVectorsShardRequest request,
        ShardId shardId,
        ActionListener<MultiTermVectorsShardResponse> listener
    ) throws IOException {
        boolean ensureDocsSearchable = DiscoveryNode.isStateless(clusterService.getSettings())
            && request.requests.stream().anyMatch(r -> r.realtime() && r.id() != null && r.id().isEmpty() == false);
        if (ensureDocsSearchable) {
            client.executeLocally(TransportEnsureDocsSearchableAction.TYPE, request, listener.delegateFailureAndWrap((l, r) -> {
                if (r.segmentGeneration() == -1) {
                    // Nothing to wait for, just handle the term vector request locally.
                    super.asyncShardOperation(request, shardId, l);
                } else {
                    assert r.segmentGeneration() > -1L : r.segmentGeneration();
                    assert r.primaryTerm() > Engine.UNKNOWN_PRIMARY_TERM : r.primaryTerm();
                    IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                    IndexShard indexShard = indexService.getShard(shardId.id());
                    final ActionListener<Long> termAndGenerationListener = ContextPreservingActionListener.wrapPreservingContext(
                        l.delegateFailureAndWrap((ll, aLong) -> super.asyncShardOperation(request, shardId, ll)),
                        threadPool.getThreadContext()
                    );
                    indexShard.waitForPrimaryTermAndGeneration(r.primaryTerm(), r.segmentGeneration(), termAndGenerationListener);
                }
            }));
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    @Override
    protected MultiTermVectorsShardResponse shardOperation(MultiTermVectorsShardRequest request, ShardId shardId) {
        final MultiTermVectorsShardResponse response = new MultiTermVectorsShardResponse();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        for (int i = 0; i < request.locations.size(); i++) {
            TermVectorsRequest termVectorsRequest = request.requests.get(i);
            try {
                TermVectorsResponse termVectorsResponse = TermVectorsService.getTermVectors(indexShard, termVectorsRequest);
                response.add(request.locations.get(i), termVectorsResponse);
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> format("%s failed to execute multi term vectors for [%s]", shardId, termVectorsRequest.id()), e);
                    response.add(
                        request.locations.get(i),
                        new MultiTermVectorsResponse.Failure(request.index(), termVectorsRequest.id(), e)
                    );
                }
            }
        }

        return response;
    }
}
