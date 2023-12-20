/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class DownsampleShardPersistentTaskExecutor extends PersistentTasksExecutor<DownsampleShardTaskParams> {
    private static final Logger LOGGER = LogManager.getLogger(DownsampleShardPersistentTaskExecutor.class);
    private final Client client;

    public DownsampleShardPersistentTaskExecutor(final Client client, final String taskName, final String executorName) {
        super(taskName, executorName);
        this.client = Objects.requireNonNull(client);
    }

    @Override
    protected void nodeOperation(
        final AllocatedPersistentTask task,
        final DownsampleShardTaskParams params,
        final PersistentTaskState state
    ) {
        // NOTE: query the downsampling target index so that we can start the downsampling task from the latest indexed tsid.
        final SearchRequest searchRequest = new SearchRequest(params.downsampleIndex());
        searchRequest.source().sort(TimeSeriesIdFieldMapper.NAME, SortOrder.DESC).size(1);
        searchRequest.preference("_shards:" + params.shardId().id());
        client.search(
            searchRequest,
            ActionListener.wrap(
                searchResponse -> delegate(task, params, searchResponse.getHits().getHits()),
                e -> delegate(task, params, new SearchHit[] {})
            )
        );
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        final String type,
        final String action,
        final TaskId parentTaskId,
        final PersistentTasksCustomMetadata.PersistentTask<DownsampleShardTaskParams> taskInProgress,
        final Map<String, String> headers
    ) {
        final DownsampleShardTaskParams params = taskInProgress.getParams();
        return new DownsampleShardTask(
            id,
            type,
            action,
            parentTaskId,
            params.downsampleIndex(),
            params.indexStartTimeMillis(),
            params.indexEndTimeMillis(),
            params.downsampleConfig(),
            headers,
            params.shardId()
        );
    }

    @Override
    public void validate(DownsampleShardTaskParams params, ClusterState clusterState) {
        // This is just a pre-check, but doesn't prevent from avoiding from aborting the task when source index disappeared
        // after initial creation of the persistent task.
        var indexShardRouting = clusterState.routingTable().shardRoutingTable(params.shardId().getIndexName(), params.shardId().id());
        if (indexShardRouting == null) {
            throw new ShardNotFoundException(params.shardId());
        }
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        final DownsampleShardTaskParams params,
        final Collection<DiscoveryNode> candidateNodes,
        final ClusterState clusterState
    ) {
        // NOTE: downsampling works by running a task per each shard of the source index.
        // Here we make sure we assign the task to the actual node holding the shard identified by
        // the downsampling task shard id.
        final ShardId shardId = params.shardId();

        // If during re-assignment the source index was deleted, then we need to break out.
        // Returning NO_NODE_FOUND just keeps the persistent task until the source index appears again (which would never happen)
        // So let's return a node and then in the node operation we would just fail and stop this persistent task
        var indexShardRouting = clusterState.routingTable().shardRoutingTable(params.shardId().getIndexName(), params.shardId().id());
        if (indexShardRouting == null) {
            var node = selectLeastLoadedNode(clusterState, candidateNodes, DiscoveryNode::canContainData);
            return new PersistentTasksCustomMetadata.Assignment(node.getId(), "a node to fail and stop this persistent task");
        }

        final ShardRouting shardRouting = indexShardRouting.primaryShard();
        if (shardRouting.started() == false) {
            return NO_NODE_FOUND;
        }

        return candidateNodes.stream()
            .filter(candidateNode -> candidateNode.getId().equals(shardRouting.currentNodeId()))
            .findAny()
            .map(
                node -> new PersistentTasksCustomMetadata.Assignment(
                    node.getId(),
                    "downsampling using node holding shard [" + shardId + "]"
                )
            )
            .orElse(NO_NODE_FOUND);
    }

    @Override
    public String getExecutor() {
        // The delegate action forks to the a downsample thread:
        return ThreadPool.Names.SAME;
    }

    private void delegate(
        final AllocatedPersistentTask task,
        final DownsampleShardTaskParams params,
        final SearchHit[] lastDownsampledTsidHits
    ) {
        client.execute(
            DelegatingAction.INSTANCE,
            new DelegatingAction.Request((DownsampleShardTask) task, lastDownsampledTsidHits, params),
            ActionListener.wrap(empty -> {}, e -> {
                LOGGER.error("error while delegating", e);
                markAsFailed(task, e);
            })
        );
    }

    static void realNodeOperation(
        Client client,
        IndicesService indicesService,
        DownsampleShardTask task,
        DownsampleShardTaskParams params,
        SearchHit[] lastDownsampleTsidHits
    ) {
        client.threadPool().executor(Downsample.DOWNSAMPLE_TASK_THREAD_POOL_NAME).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                markAsFailed(task, e);
            }

            @Override
            protected void doRun() throws Exception {
                final var initialState = lastDownsampleTsidHits.length == 0
                    ? new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null)
                    : new DownsampleShardPersistentTaskState(
                        DownsampleShardIndexerStatus.STARTED,
                        Arrays.stream(lastDownsampleTsidHits).findFirst().get().field("_tsid").getValue()
                    );
                try {
                    final var downsampleShardIndexer = new DownsampleShardIndexer(
                        task,
                        client,
                        indicesService.indexService(params.shardId().getIndex()),
                        params.shardId(),
                        params.downsampleIndex(),
                        params.downsampleConfig(),
                        params.metrics(),
                        params.labels(),
                        initialState
                    );
                    downsampleShardIndexer.execute();
                    task.markAsCompleted();
                } catch (final DownsampleShardIndexerException e) {
                    if (e.isRetriable()) {
                        LOGGER.warn("Downsampling task [" + task.getPersistentTaskId() + " retriable failure [" + e.getMessage() + "]");
                        task.markAsLocallyAborted(e.getMessage());
                    } else {
                        LOGGER.error(
                            "Downsampling task [" + task.getPersistentTaskId() + " non retriable failure [" + e.getMessage() + "]"
                        );
                        markAsFailed(task, e);
                    }
                } catch (final Exception e) {
                    LOGGER.error("Downsampling task [" + task.getPersistentTaskId() + " non-retriable failure [" + e.getMessage() + "]");
                    markAsFailed(task, e);
                }
            }
        });
    }

    private static void markAsFailed(AllocatedPersistentTask task, Exception e) {
        task.updatePersistentTaskState(
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.FAILED, null),
            ActionListener.running(() -> task.markAsFailed(e))
        );
    }

    // This is needed for FLS/DLS to work correctly. The _indices_permissions in the thread local aren't set if an searcher is acquired
    // directly from this persistent task executor. By delegating to this action (with a request that implements IndicesRequest) the
    // security thread local will be setup correctly so that we avoid this error:
    // org.elasticsearch.ElasticsearchSecurityException: no indices permissions found
    public static class DelegatingAction extends ActionType<ActionResponse.Empty> {

        public static final DelegatingAction INSTANCE = new DelegatingAction();
        public static final String NAME = "indices:data/read/downsample_delegate";

        private DelegatingAction() {
            super(NAME, in -> new ActionResponse.Empty());
        }

        public static class Request extends ActionRequest implements IndicesRequest {

            private final DownsampleShardTask task;
            private final SearchHit[] lastDownsampleTsidHits;
            private final DownsampleShardTaskParams params;

            public Request(DownsampleShardTask task, SearchHit[] lastDownsampleTsidHits, DownsampleShardTaskParams params) {
                this.task = task;
                this.lastDownsampleTsidHits = lastDownsampleTsidHits;
                this.params = params;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public String[] indices() {
                return new String[] { params.shardId().getIndexName() };
            }

            @Override
            public IndicesOptions indicesOptions() {
                return IndicesOptions.STRICT_EXPAND_OPEN;
            }

            @Override
            public void writeTo(StreamOutput out) {
                throw new IllegalStateException("request should stay local");
            }
        }

        public static class TA extends TransportAction<Request, ActionResponse.Empty> {

            private final Client client;
            private final IndicesService indicesService;

            @Inject
            public TA(TransportService transportService, ActionFilters actionFilters, Client client, IndicesService indicesService) {
                super(NAME, actionFilters, transportService.getTaskManager());
                this.client = client;
                this.indicesService = indicesService;
            }

            @Override
            protected void doExecute(Task t, Request request, ActionListener<ActionResponse.Empty> listener) {
                realNodeOperation(client, indicesService, request.task, request.params, request.lastDownsampleTsidHits);
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        }
    }
}
