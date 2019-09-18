/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.transform.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.transform.persistence.DataFrameTransformsConfigManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class ClientDataFrameIndexerBuilder {
    private Client client;
    private DataFrameTransformsConfigManager transformsConfigManager;
    private DataFrameTransformsCheckpointService transformsCheckpointService;
    private DataFrameAuditor auditor;
    private Map<String, String> fieldMappings;
    private TransformConfig transformConfig;
    private TransformIndexerStats initialStats;
    private IndexerState indexerState = IndexerState.STOPPED;
    private TransformIndexerPosition initialPosition;
    private TransformProgress progress;
    private TransformCheckpoint lastCheckpoint;
    private TransformCheckpoint nextCheckpoint;

    ClientDataFrameIndexerBuilder() {
        this.initialStats = new TransformIndexerStats();
    }

    ClientDataFrameIndexer build(DataFrameTransformTask parentTask) {
        CheckpointProvider checkpointProvider = transformsCheckpointService.getCheckpointProvider(transformConfig);

        return new ClientDataFrameIndexer(this.transformsConfigManager,
            checkpointProvider,
            new AtomicReference<>(this.indexerState),
            this.initialPosition,
            this.client,
            this.auditor,
            this.initialStats,
            this.transformConfig,
            this.fieldMappings,
            this.progress,
            this.lastCheckpoint,
            this.nextCheckpoint,
            parentTask);
    }

    ClientDataFrameIndexerBuilder setClient(Client client) {
        this.client = client;
        return this;
    }

    ClientDataFrameIndexerBuilder setTransformsConfigManager(DataFrameTransformsConfigManager transformsConfigManager) {
        this.transformsConfigManager = transformsConfigManager;
        return this;
    }

    ClientDataFrameIndexerBuilder setTransformsCheckpointService(DataFrameTransformsCheckpointService transformsCheckpointService) {
        this.transformsCheckpointService = transformsCheckpointService;
        return this;
    }

    ClientDataFrameIndexerBuilder setAuditor(DataFrameAuditor auditor) {
        this.auditor = auditor;
        return this;
    }

    ClientDataFrameIndexerBuilder setFieldMappings(Map<String, String> fieldMappings) {
        this.fieldMappings = fieldMappings;
        return this;
    }

    ClientDataFrameIndexerBuilder setTransformConfig(TransformConfig transformConfig) {
        this.transformConfig = transformConfig;
        return this;
    }

    TransformConfig getTransformConfig() {
        return this.transformConfig;
    }

    ClientDataFrameIndexerBuilder setInitialStats(TransformIndexerStats initialStats) {
        this.initialStats = initialStats;
        return this;
    }

    ClientDataFrameIndexerBuilder setIndexerState(IndexerState indexerState) {
        this.indexerState = indexerState;
        return this;
    }

    ClientDataFrameIndexerBuilder setInitialPosition(TransformIndexerPosition initialPosition) {
        this.initialPosition = initialPosition;
        return this;
    }

    ClientDataFrameIndexerBuilder setProgress(TransformProgress progress) {
        this.progress = progress;
        return this;
    }

    ClientDataFrameIndexerBuilder setLastCheckpoint(TransformCheckpoint lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
        return this;
    }

    ClientDataFrameIndexerBuilder setNextCheckpoint(TransformCheckpoint nextCheckpoint) {
        this.nextCheckpoint = nextCheckpoint;
        return this;
    }
}