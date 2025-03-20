/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Base class for requests that should be executed on all shards of an index or several indices.
 * This action sends shard requests to all primary shards of the indices and they are then replicated like write requests
 */
public abstract class TransportBroadcastReplicationAction<
    Request extends BroadcastRequest<Request>,
    Response extends BaseBroadcastResponse,
    ShardRequest extends ReplicationRequest<ShardRequest>,
    ShardResponse extends ReplicationResponse> extends HandledTransportAction<Request, Response> {

    private final ActionType<ShardResponse> replicatedBroadcastShardAction;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final NodeClient client;
    private final Executor executor;
    private final ProjectResolver projectResolver;

    public TransportBroadcastReplicationAction(
        String name,
        Writeable.Reader<Request> requestReader,
        ClusterService clusterService,
        TransportService transportService,
        NodeClient client,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionType<ShardResponse> replicatedBroadcastShardAction,
        Executor executor,
        ProjectResolver projectResolver
    ) {
        super(name, transportService, actionFilters, requestReader, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.replicatedBroadcastShardAction = replicatedBroadcastShardAction;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        executor.execute(ActionRunnable.wrap(listener, createAsyncAction(task, request)));
    }

    private CheckedConsumer<ActionListener<Response>, Exception> createAsyncAction(Task task, Request request) {
        return new CheckedConsumer<ActionListener<Response>, Exception>() {

            private int totalShardCopyCount;
            private int successShardCopyCount;
            private final List<DefaultShardOperationFailedException> allFailures = new ArrayList<>();

            @Override
            public void accept(ActionListener<Response> listener) {
                assert totalShardCopyCount == 0 && successShardCopyCount == 0 && allFailures.isEmpty() : "shouldn't call this twice";

                final ClusterState clusterState = clusterService.state();
                final ProjectMetadata project = projectResolver.getProjectMetadata(clusterState);
                final List<ShardId> shards = shards(request, project, clusterState.routingTable(project.id()));
                final Map<String, IndexMetadata> indexMetadataByName = project.indices();

                try (var refs = new RefCountingRunnable(() -> finish(listener))) {
                    for (final ShardId shardId : shards) {
                        // NB This sends O(#shards) requests in a tight loop; TODO add some throttling here?
                        shardExecute(
                            task,
                            request,
                            shardId,
                            ActionListener.releaseAfter(new ReplicationResponseActionListener(shardId, indexMetadataByName), refs.acquire())
                        );
                    }
                }
            }

            private synchronized void addShardResponse(int numCopies, int successful, List<DefaultShardOperationFailedException> failures) {
                totalShardCopyCount += numCopies;
                successShardCopyCount += successful;
                allFailures.addAll(failures);
            }

            void finish(ActionListener<Response> listener) {
                // no need for synchronized here, the RefCountingRunnable guarantees that all the addShardResponse calls happen-before here
                logger.trace("{}: got all shard responses", actionName);
                listener.onResponse(newResponse(successShardCopyCount, allFailures.size(), totalShardCopyCount, allFailures));
            }

            class ReplicationResponseActionListener implements ActionListener<ShardResponse> {
                private final ShardId shardId;
                private final Map<String, IndexMetadata> indexMetadataByName;

                ReplicationResponseActionListener(ShardId shardId, Map<String, IndexMetadata> indexMetadataByName) {
                    this.shardId = shardId;
                    this.indexMetadataByName = indexMetadataByName;
                }

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
                    final List<DefaultShardOperationFailedException> result;
                    if (TransportActions.isShardNotAvailableException(e)) {
                        result = List.of();
                    } else {
                        final var failures = new DefaultShardOperationFailedException[numCopies];
                        Arrays.fill(
                            failures,
                            new DefaultShardOperationFailedException(new BroadcastShardOperationFailedException(shardId, e))
                        );
                        result = Arrays.asList(failures);
                    }
                    addShardResponse(numCopies, 0, result);
                }
            }

        };
    }

    protected void shardExecute(Task task, Request request, ShardId shardId, ActionListener<ShardResponse> shardActionListener) {
        assert Transports.assertNotTransportThread("may hit all the shards");
        ShardRequest shardRequest = newShardRequest(request, shardId);
        shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.executeLocally(replicatedBroadcastShardAction, shardRequest, shardActionListener);
    }

    /**
     * @return all shard ids the request should run on
     */
    protected List<ShardId> shards(Request request, ProjectMetadata project, RoutingTable indexRoutingTables) {
        assert Transports.assertNotTransportThread("may hit all the shards");
        List<ShardId> shardIds = new ArrayList<>();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(project, request);
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = project.indices().get(index);
            if (indexMetadata != null) {
                final IndexRoutingTable indexRoutingTable = indexRoutingTables.indicesRouting().get(index);
                for (int i = 0; i < indexRoutingTable.size(); i++) {
                    shardIds.add(indexRoutingTable.shard(i).shardId());
                }
            }
        }
        return shardIds;
    }

    protected abstract ShardRequest newShardRequest(Request request, ShardId shardId);

    protected abstract Response newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    );

}
