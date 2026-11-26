/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SearchShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Collects {@link TsidPrefixHistogram} statistics for planning.
 * Document-level security and alias filters could be ignored, as the histogram is an estimate and returns no documents.
 */
public class TransportTsidPrefixHistogramAction extends HandledTransportAction<TsidPrefixHistogramRequest, TsidPrefixHistogramResponse> {

    public static final ActionType<TsidPrefixHistogramResponse> TYPE = new ActionType<>("indices:data/read/esql/tsid_prefix_histograms");
    static final String NODE_ACTION = TYPE.name() + "[n]";

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ProjectResolver projectResolver;
    private final IndicesService indicesService;
    private final Executor coordinationExecutor;

    @Inject
    public TransportTsidPrefixHistogramAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver,
        IndicesService indicesService
    ) {
        super(
            TYPE.name(),
            transportService,
            actionFilters,
            TsidPrefixHistogramRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
        );
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.projectResolver = projectResolver;
        this.indicesService = indicesService;
        this.coordinationExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION);
        transportService.registerRequestHandler(
            NODE_ACTION,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            TsidPrefixHistogramNodeRequest::new,
            (request, channel, task) -> ActionListener.completeWith(
                new ChannelActionListener<>(channel),
                () -> collectNode(request, (CancellableTask) task)
            )
        );
    }

    @Override
    protected void doExecute(Task task, TsidPrefixHistogramRequest request, ActionListener<TsidPrefixHistogramResponse> listener) {
        ClusterState clusterState = clusterService.state();
        var project = projectResolver.getProjectState(clusterState);
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        // remote concrete indices may carry their cluster prefix, which this cluster's names do not
        Set<String> targets = Arrays.stream(request.concreteIndices)
            .map(index -> RemoteClusterAware.splitIndexName(index).indexExpression())
            .collect(Collectors.toSet());
        String[] indices = Arrays.stream(indexNameExpressionResolver.concreteIndexNames(clusterState, request))
            .filter(targets::contains)
            .toArray(String[]::new);
        ClusterBlockException blocked = clusterState.blocks().indicesBlockedException(project.projectId(), ClusterBlockLevel.READ, indices);
        if (blocked != null) {
            throw blocked;
        }
        var shards = clusterService.operationRouting().searchShards(project, indices, null, null);
        var execution = new Execution((CancellableTask) task, request, shards, listener);
        execution.run(clusterState.nodes());
    }

    private TsidPrefixHistogramNodeResponse collectNode(TsidPrefixHistogramNodeRequest request, CancellableTask task) {
        if (request.shards().isEmpty()) {
            // the coordinator never sends an empty group, so security resolved none of the query's indices on this node
            throw new ElasticsearchSecurityException("no authorized indices on node for " + NODE_ACTION, RestStatus.FORBIDDEN);
        }
        TsidPrefixHistogram histogram = TsidPrefixHistogram.EMPTY;
        final Map<ShardId, Exception> failures = new HashMap<>();
        for (ShardId shardId : request.shards()) {
            task.ensureNotCancelled();
            try {
                var indexService = indicesService.indexServiceSafe(shardId.getIndex());
                var shardHistogram = TsidPrefixHistogramCollector.collectShard(indexService, shardId, request, task);
                histogram = TsidPrefixHistogram.merge(histogram, shardHistogram);
            } catch (TaskCancelledException e) {
                throw e;
            } catch (Exception e) {
                failures.put(shardId, e);
            }
        }
        return new TsidPrefixHistogramNodeResponse(histogram.capped(TsidPrefixHistogram.MAX_CELLS), failures);
    }

    private final class Execution {
        private final CancellableTask task;
        private final TsidPrefixHistogramRequest request;
        private final List<SearchShardRouting> shards;
        private final ActionListener<TsidPrefixHistogramResponse> listener;
        private final Exception[] failures;
        private TsidPrefixHistogram histogram = TsidPrefixHistogram.EMPTY;

        Execution(
            CancellableTask task,
            TsidPrefixHistogramRequest request,
            List<SearchShardRouting> shards,
            ActionListener<TsidPrefixHistogramResponse> listener
        ) {
            this.task = task;
            this.request = request;
            this.shards = shards;
            this.listener = listener;
            this.failures = new Exception[shards.size()];
        }

        void run(DiscoveryNodes nodes) {
            Map<DiscoveryNode, List<Integer>> byNode = new LinkedHashMap<>();
            for (int shard = 0; shard < shards.size(); shard++) {
                ShardRouting copy = shards.get(shard).nextOrNull();
                DiscoveryNode node = copy == null ? null : nodes.get(copy.currentNodeId());
                if (node != null) {
                    byNode.computeIfAbsent(node, n -> new ArrayList<>()).add(shard);
                } else {
                    failures[shard] = new NoShardAvailableActionException(shards.get(shard).shardId());
                }
            }
            // TODO: handle retry
            try (var refs = new RefCountingRunnable(this::onDone)) {
                for (var entry : byNode.entrySet()) {
                    sendNodeRequest(entry.getKey(), entry.getValue(), refs.acquire());
                }
            }
        }

        private void sendNodeRequest(DiscoveryNode node, List<Integer> group, Releasable done) {
            List<ShardId> shardIds = group.stream().map(shard -> shards.get(shard).shardId()).toList();
            transportService.sendChildRequest(
                node,
                NODE_ACTION,
                new TsidPrefixHistogramNodeRequest(request, shardIds),
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(ActionListener.releaseAfter(new ActionListener<>() {
                    @Override
                    public void onResponse(TsidPrefixHistogramNodeResponse response) {
                        onNodeResponse(group, shardIds, response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onNodeFailure(group, e);
                    }
                }, done), TsidPrefixHistogramNodeResponse::new, coordinationExecutor)
            );
        }

        private void onNodeResponse(List<Integer> group, List<ShardId> shardIds, TsidPrefixHistogramNodeResponse response) {
            synchronized (this) {
                histogram = TsidPrefixHistogram.merge(histogram, response.histogram);
                for (int i = 0; i < group.size(); i++) {
                    failures[group.get(i)] = response.failures.get(shardIds.get(i));
                }
            }
        }

        private void onNodeFailure(List<Integer> group, Exception e) {
            synchronized (this) {
                for (int shard : group) {
                    failures[shard] = e;
                }
            }
        }

        private void onDone() {
            if (task.notifyIfCancelled(listener)) {
                return;
            }
            ActionListener.completeWith(listener, () -> {
                List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
                for (int shard = 0; shard < failures.length; shard++) {
                    if (failures[shard] != null) {
                        ShardId shardId = shards.get(shard).shardId();
                        shardFailures.add(new DefaultShardOperationFailedException(shardId.getIndexName(), shardId.id(), failures[shard]));
                    }
                }
                return new TsidPrefixHistogramResponse(
                    shards.size(),
                    shards.size() - shardFailures.size(),
                    shardFailures.size(),
                    shardFailures,
                    histogram.capped(TsidPrefixHistogram.MAX_CELLS),
                    Map.of()
                );
            });
        }
    }
}
