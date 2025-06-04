/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TransportAnalyzeIndexDiskUsageAction extends TransportBroadcastAction<
    AnalyzeIndexDiskUsageRequest,
    AnalyzeIndexDiskUsageResponse,
    AnalyzeDiskUsageShardRequest,
    AnalyzeDiskUsageShardResponse> {
    public static final ActionType<AnalyzeIndexDiskUsageResponse> TYPE = new ActionType<>("indices:admin/analyze_disk_usage");
    private final IndicesService indicesService;
    private final ProjectResolver projectResolver;
    private final ThreadPool threadPool;

    @Inject
    public TransportAnalyzeIndexDiskUsageAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indexServices,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            AnalyzeIndexDiskUsageRequest::new,
            AnalyzeDiskUsageShardRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.ANALYZE)
        );
        this.indicesService = indexServices;
        this.projectResolver = projectResolver;
        this.threadPool = transportService.getThreadPool();
    }

    @Override
    protected void doExecute(Task task, AnalyzeIndexDiskUsageRequest request, ActionListener<AnalyzeIndexDiskUsageResponse> listener) {
        new LimitingRequestPerNodeBroadcastAction(task, request, listener, 5).start();
    }

    private record ShardRequest(
        DiscoveryNode node,
        AnalyzeDiskUsageShardRequest shardRequest,
        ActionListener<AnalyzeDiskUsageShardResponse> handler
    ) {

    }

    final class LimitingRequestPerNodeBroadcastAction extends AsyncBroadcastAction {
        private final Queue<ShardRequest> queue = new LinkedList<>();
        private final Map<DiscoveryNode, AtomicInteger> sendingCounters = ConcurrentCollections.newConcurrentMap();
        private final int maxConcurrentRequestsPerNode;

        LimitingRequestPerNodeBroadcastAction(
            Task task,
            AnalyzeIndexDiskUsageRequest request,
            ActionListener<AnalyzeIndexDiskUsageResponse> listener,
            int maxConcurrentRequestsPerNode
        ) {
            super(task, request, listener);
            this.maxConcurrentRequestsPerNode = maxConcurrentRequestsPerNode;
        }

        private void trySendRequests() {
            assert Thread.holdsLock(this) == false;
            final List<ShardRequest> readyRequests = new ArrayList<>();
            synchronized (this) {
                final Iterator<ShardRequest> it = queue.iterator();
                while (it.hasNext()) {
                    final ShardRequest r = it.next();
                    final AtomicInteger sending = sendingCounters.computeIfAbsent(r.node, k -> new AtomicInteger());
                    assert 0 <= sending.get() && sending.get() <= maxConcurrentRequestsPerNode : sending;
                    if (sending.get() < maxConcurrentRequestsPerNode) {
                        sending.incrementAndGet();
                        readyRequests.add(r);
                        it.remove();
                    }
                }
            }
            if (readyRequests.isEmpty()) {
                return;
            }
            final Thread sendingThread = Thread.currentThread();
            for (ShardRequest r : readyRequests) {
                super.sendShardRequest(
                    r.node,
                    r.shardRequest,
                    ActionListener.runAfter(r.handler, () -> onRequestResponded(sendingThread, r.node))
                );
            }
        }

        private void onRequestResponded(Thread sendingThread, DiscoveryNode node) {
            final AtomicInteger sending = sendingCounters.get(node);
            assert sending != null && 1 <= sending.get() && sending.get() <= maxConcurrentRequestsPerNode : sending;
            sending.decrementAndGet();
            // fork to avoid StackOverflow
            if (sendingThread == Thread.currentThread()) {
                threadPool.generic().execute(this::trySendRequests);
            } else {
                trySendRequests();
            }
        }

        @Override
        protected synchronized void sendShardRequest(
            DiscoveryNode node,
            AnalyzeDiskUsageShardRequest shardRequest,
            ActionListener<AnalyzeDiskUsageShardResponse> listener
        ) {
            queue.add(new ShardRequest(node, shardRequest, listener));
        }

        @Override
        public void start() {
            super.start();
            trySendRequests();
        }
    }

    @Override
    protected AnalyzeDiskUsageShardRequest newShardRequest(int numShards, ShardRouting shard, AnalyzeIndexDiskUsageRequest request) {
        return new AnalyzeDiskUsageShardRequest(shard.shardId(), request);
    }

    @Override
    protected AnalyzeDiskUsageShardResponse readShardResponse(StreamInput in) throws IOException {
        return new AnalyzeDiskUsageShardResponse(in);
    }

    @Override
    protected AnalyzeDiskUsageShardResponse shardOperation(AnalyzeDiskUsageShardRequest request, Task task) throws IOException {
        final ShardId shardId = request.shardId();
        assert task instanceof CancellableTask : "AnalyzeDiskUsageShardRequest must create a cancellable task";
        final CancellableTask cancellableTask = (CancellableTask) task;
        final Runnable checkForCancellation = cancellableTask::ensureNotCancelled;
        final IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(request.flush)) {
            final IndexDiskUsageStats stats = IndexDiskUsageAnalyzer.analyze(shardId, commitRef.getIndexCommit(), checkForCancellation);
            return new AnalyzeDiskUsageShardResponse(shardId, stats);
        }
    }

    @Override
    protected AnalyzeIndexDiskUsageResponse newResponse(
        AnalyzeIndexDiskUsageRequest request,
        AtomicReferenceArray<?> shardsResponses,
        ClusterState clusterState
    ) {
        int successfulShards = 0;
        final List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        final Map<String, IndexDiskUsageStats> combined = new HashMap<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            final Object r = shardsResponses.get(i);
            if (r instanceof AnalyzeDiskUsageShardResponse resp) {
                ++successfulShards;
                combined.compute(resp.getIndex(), (k, v) -> v == null ? resp.stats : v.add(resp.stats));
            } else if (r instanceof DefaultShardOperationFailedException e) {
                shardFailures.add(e);
            } else if (r instanceof Exception e) {
                shardFailures.add(new DefaultShardOperationFailedException(ExceptionsHelper.convertToElastic(e)));
            } else {
                assert false : "unknown response [" + r + "]";
                throw new IllegalStateException("unknown response [" + r + "]");
            }
        }
        return new AnalyzeIndexDiskUsageResponse(shardsResponses.length(), successfulShards, shardFailures.size(), shardFailures, combined);
    }

    @Override
    protected List<ShardIterator> shards(ClusterState clusterState, AnalyzeIndexDiskUsageRequest request, String[] concreteIndices) {
        ProjectState project = projectResolver.getProjectState(clusterState);
        final List<ShardIterator> groups = clusterService.operationRouting().searchShards(project, concreteIndices, null, null);

        for (ShardIterator group : groups) {
            // fails fast if any non-active groups
            if (group.size() == 0) {
                throw new NoShardAvailableActionException(group.shardId());
            }
        }
        return groups;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, AnalyzeIndexDiskUsageRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, AnalyzeIndexDiskUsageRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
