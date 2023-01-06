/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 */
public abstract class TransportBroadcastReplicationAction<
    Request extends BroadcastRequest<Request>,
    Response extends BaseBroadcastResponse,
    ShardRequest extends ReplicationRequest<ShardRequest>,
    ShardResponse extends ReplicationResponse> extends HandledTransportAction<Request, Response> {

    static int MAX_REQUESTS_PER_NODE = 10; // The REFRESH threadpool maxes out at 10 by default so this is enough to keep everyone busy.

    private final ActionType<ShardResponse> replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final String executor;
    private final NodeClient client;

    protected TransportBroadcastReplicationAction(
        String name,
        Writeable.Reader<Request> requestReader,
        ClusterService clusterService,
        TransportService transportService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionType<ShardResponse> replicatedBroadcastShardAction,
        String executor
    ) {
        super(name, transportService, actionFilters, requestReader, executor);
        this.client = client;
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var clusterState = clusterService.state();
        final var context = new Context(task, request, clusterState.metadata().indices(), listener);
        ThrottledIterator.run(
            shardIds(request, clusterState),
            context::processShard,
            clusterState.nodes().getDataNodes().size() * MAX_REQUESTS_PER_NODE,
            () -> {},
            context::finish
        );
    }

    protected void shardExecute(Task task, Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        assert Transports.assertNotTransportThread("per-shard requests might be high-volume");
        ShardRequest shardRequest = newShardRequest(request, shardId);
        shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.executeLocally(replicatedBroadcastShardAction, shardRequest, shardActionListener);
    }

    /**
     * @return all shard ids on which the request should run; exposed for tests
     */
    Iterator<? extends ShardId> shardIds(Request request, ClusterState clusterState) {
        var indexMetadataByName = clusterState.metadata().indices();
        return Iterators.flatMap(
            Iterators.forArray(indexNameExpressionResolver.concreteIndexNames(clusterState, request)),
            indexName -> indexShardIds(indexMetadataByName.get(indexName))
        );
    }

    private static Iterator<ShardId> indexShardIds(@Nullable IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return Collections.emptyIterator();
        }
        var shardIds = new ShardId[indexMetadata.getNumberOfShards()];
        for (int i = 0; i < shardIds.length; i++) {
            shardIds[i] = new ShardId(indexMetadata.getIndex(), i);
        }
        return Iterators.forArray(shardIds);
    }

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    protected abstract Response newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    );

    private class Context {
        private final Task task;
        private final Request request;
        private final Map<String, IndexMetadata> indexMetadataByName;
        private final ActionListener<Response> listener;

        private int totalNumCopies;
        private int totalSuccessful;
        private final List<DefaultShardOperationFailedException> allFailures = new ArrayList<>();

        Context(Task task, Request request, Map<String, IndexMetadata> indexMetadataByName, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.indexMetadataByName = indexMetadataByName;
            this.listener = listener;
        }

        void processShard(ThrottledIterator.ItemRefs refs, ShardId shardId) {
            shardExecute(
                task,
                request,
                shardId,
                new ThreadedActionListener<>(
                    logger,
                    clusterService.threadPool(),
                    executor,
                    ActionListener.releaseAfter(createListener(shardId), refs.acquire()),
                    false
                )
            );
        }

        private ActionListener<ShardResponse> createListener(ShardId shardId) {
            return new ActionListener<>() {
                @Override
                public void onResponse(ShardResponse shardResponse) {
                    assert shardResponse != null;
                    logger.trace("{}: got response from {}", actionName, shardId);
                    addShardResponse(
                        shardResponse.getShardInfo().getTotal(),
                        shardResponse.getShardInfo().getSuccessful(),
                        Arrays.stream(shardResponse.getShardInfo().getFailures())
                            .map(
                                f -> new DefaultShardOperationFailedException(
                                    new BroadcastShardOperationFailedException(shardId, f.getCause())
                                )
                            )
                            .toList()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.trace("{}: got failure from {}", actionName, shardId);
                    final int numCopies = indexMetadataByName.get(shardId.getIndexName()).getNumberOfReplicas() + 1;
                    addShardResponse(numCopies, 0, createSyntheticFailures(numCopies, e));
                }

                private List<DefaultShardOperationFailedException> createSyntheticFailures(int numCopies, Exception e) {
                    if (TransportActions.isShardNotAvailableException(e)) {
                        return List.of();
                    }

                    final var failures = new DefaultShardOperationFailedException[numCopies];
                    Arrays.fill(failures, new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(shardId, e)));
                    return Arrays.asList(failures);
                }
            };
        }

        private synchronized void addShardResponse(int numCopies, int successful, List<DefaultShardOperationFailedException> failures) {
            totalNumCopies += numCopies;
            totalSuccessful += successful;
            allFailures.addAll(failures);
        }

        void finish() {
            // no need for synchronized here, the ThrottledIterator guarantees that all the addShardResponse calls happen-before this point
            logger.trace("{}: got all shard responses", actionName);
            listener.onResponse(newResponse(totalSuccessful, allFailures.size(), totalNumCopies, allFailures));
        }
    }
}
