/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;

import java.util.concurrent.atomic.AtomicReference;

class ClientTransformIndexerBuilder {
    private ParentTaskAssigningClient parentTaskClient;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private TransformExtension transformExtension;
    private TransformServices transformServices;
    private TransformConfig transformConfig;
    private TransformIndexerStats initialStats;
    private IndexerState indexerState = IndexerState.STOPPED;
    private TransformIndexerPosition initialPosition;
    private TransformProgress progress;
    private TransformCheckpoint lastCheckpoint;
    private TransformCheckpoint nextCheckpoint;
    private SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex;
    private boolean shouldStopAtCheckpoint;

    ClientTransformIndexerBuilder() {
        this.initialStats = new TransformIndexerStats();
    }

    ClientTransformIndexer build(ThreadPool threadPool, TransformContext context) {
        CheckpointProvider checkpointProvider = transformServices.getCheckpointService()
            .getCheckpointProvider(parentTaskClient, transformConfig);

        return new ClientTransformIndexer(
            threadPool,
            clusterService,
            indexNameExpressionResolver,
            transformExtension,
            transformServices,
            checkpointProvider,
            new AtomicReference<>(this.indexerState),
            initialPosition,
            parentTaskClient,
            initialStats,
            transformConfig,
            progress,
            // If there already exists at least one checkpoint, the "from" setting is effectively ignored.
            TransformCheckpoint.isNullOrEmpty(lastCheckpoint)
                ? (context.from() != null ? TransformCheckpoint.createEmpty(context.from().toEpochMilli()) : TransformCheckpoint.EMPTY)
                : lastCheckpoint,
            TransformCheckpoint.isNullOrEmpty(nextCheckpoint) ? TransformCheckpoint.EMPTY : nextCheckpoint,
            seqNoPrimaryTermAndIndex,
            context,
            shouldStopAtCheckpoint
        );
    }

    ClientTransformIndexerBuilder setShouldStopAtCheckpoint(boolean shouldStopAtCheckpoint) {
        this.shouldStopAtCheckpoint = shouldStopAtCheckpoint;
        return this;
    }

    ClientTransformIndexerBuilder setClient(ParentTaskAssigningClient parentTaskClient) {
        this.parentTaskClient = parentTaskClient;
        return this;
    }

    ClientTransformIndexerBuilder setIndexNameExpressionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        return this;
    }

    ClientTransformIndexerBuilder setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
        return this;
    }

    ClientTransformIndexerBuilder setTransformExtension(TransformExtension transformExtension) {
        this.transformExtension = transformExtension;
        return this;
    }

    ClientTransformIndexerBuilder setTransformServices(TransformServices transformServices) {
        this.transformServices = transformServices;
        return this;
    }

    ClientTransformIndexerBuilder setTransformConfig(TransformConfig transformConfig) {
        this.transformConfig = transformConfig;
        return this;
    }

    TransformConfig getTransformConfig() {
        return this.transformConfig;
    }

    ClientTransformIndexerBuilder setInitialStats(TransformIndexerStats initialStats) {
        this.initialStats = initialStats;
        return this;
    }

    ClientTransformIndexerBuilder setIndexerState(IndexerState indexerState) {
        this.indexerState = indexerState;
        return this;
    }

    ClientTransformIndexerBuilder setInitialPosition(TransformIndexerPosition initialPosition) {
        this.initialPosition = initialPosition;
        return this;
    }

    ClientTransformIndexerBuilder setProgress(TransformProgress progress) {
        this.progress = progress;
        return this;
    }

    ClientTransformIndexerBuilder setLastCheckpoint(TransformCheckpoint lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
        return this;
    }

    ClientTransformIndexerBuilder setNextCheckpoint(TransformCheckpoint nextCheckpoint) {
        this.nextCheckpoint = nextCheckpoint;
        return this;
    }

    ClientTransformIndexerBuilder setSeqNoPrimaryTermAndIndex(SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex) {
        this.seqNoPrimaryTermAndIndex = seqNoPrimaryTermAndIndex;
        return this;
    }

}
