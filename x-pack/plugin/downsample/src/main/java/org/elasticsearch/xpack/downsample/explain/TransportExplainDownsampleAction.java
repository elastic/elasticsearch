/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.explain;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.DownsampleTaskStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateStore.Versioned;
import org.elasticsearch.xpack.downsample.orchestration.IndexTaskDoc;
import org.elasticsearch.xpack.downsample.orchestration.ShardTaskDoc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the requested indices on the local cluster state, keeps the downsampled ones, and reads their operation and shard task
 * documents from the state index of the project, in real time.
 */
public class TransportExplainDownsampleAction extends TransportLocalProjectMetadataAction<
    ExplainDownsampleAction.Request,
    ExplainDownsampleAction.Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DownsampleStateStore store;
    private final ThreadPool threadPool;

    @Inject
    public TransportExplainDownsampleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ProjectResolver projectResolver,
        DownsampleStateStore store
    ) {
        super(
            ExplainDownsampleAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.store = store;
        this.threadPool = threadPool;
    }

    @Override
    protected ClusterBlockException checkBlock(ExplainDownsampleAction.Request request, ProjectState project) {
        return project.blocks()
            .indicesBlockedException(
                project.projectId(),
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(project.metadata(), request)
            );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        ExplainDownsampleAction.Request request,
        ProjectState project,
        ActionListener<ExplainDownsampleAction.Response> listener
    ) {
        final ProjectId projectId = project.projectId();
        final Set<String> sourceIndices = Set.of(request.sourceIndices());
        final List<IndexMetadata> downsampled = new ArrayList<>();
        for (String name : indexNameExpressionResolver.concreteIndexNames(project.metadata(), request)) {
            IndexMetadata index = project.metadata().index(name);
            if (IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(index.getSettings()) == DownsampleTaskStatus.UNKNOWN) {
                continue;
            }
            // the source is matched by the name recorded on the downsampled index, so it works after the source was deleted too
            if (sourceIndices.isEmpty() || sourceIndices.contains(IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(index.getSettings()))) {
                downsampled.add(index);
            }
        }
        if (downsampled.isEmpty()) {
            listener.onResponse(new ExplainDownsampleAction.Response(List.of()));
            return;
        }
        final long now = threadPool.absoluteTimeInMillis();
        final DownsampleStateStore.ReadView reader = store.forReaders();
        reader.getIndexTasks(
            projectId,
            downsampled.stream().map(index -> index.getIndex().getName()).toList(),
            listener.delegateFailureAndWrap((l, operations) -> {
                final Map<String, IndexDownsampleExplanation> explanations = new ConcurrentHashMap<>();
                try (RefCountingListener refs = new RefCountingListener(l.map(ignored -> {
                    ((CancellableTask) task).ensureNotCancelled();
                    return new ExplainDownsampleAction.Response(
                        explanations.values().stream().sorted(Comparator.comparing(IndexDownsampleExplanation::index)).toList()
                    );
                }))) {
                    for (IndexMetadata index : downsampled) {
                        String name = index.getIndex().getName();
                        IndexTaskDoc operation = Optional.ofNullable(operations.get(name))
                            .map(Versioned::doc)
                            // an operation recorded under the same name for an index that was since deleted is not about this one
                            .filter(op -> index.getIndexUUID().equals(op.targetIndexUuid()))
                            .orElse(null);
                        if (operation == null || IndexDownsampleExplanation.hasShardTasks(operation) == false) {
                            explanations.put(name, IndexDownsampleExplanation.of(index, operation, List.of(), now));
                            continue;
                        }
                        reader.getShardTasks(projectId, name, index.getIndexUUID(), index.getNumberOfShards(), refs.acquire(found -> {
                            List<ShardTaskDoc> shards = found.stream().flatMap(Optional::stream).map(Versioned::doc).toList();
                            explanations.put(name, IndexDownsampleExplanation.of(index, operation, shards, now));
                        }));
                    }
                }
            })
        );
    }
}
