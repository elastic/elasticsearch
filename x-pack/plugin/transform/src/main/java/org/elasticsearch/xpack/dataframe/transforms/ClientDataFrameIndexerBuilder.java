/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.dataframe.checkpoint.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class ClientDataFrameIndexerBuilder {
    private Client client;
    private DataFrameTransformsConfigManager transformsConfigManager;
    private DataFrameTransformsCheckpointService transformsCheckpointService;
    private DataFrameAuditor auditor;
    private Map<String, String> fieldMappings;
    private DataFrameTransformConfig transformConfig;
    private DataFrameIndexerTransformStats initialStats;
    private IndexerState indexerState = IndexerState.STOPPED;
    private DataFrameIndexerPosition initialPosition;
    private DataFrameTransformProgress progress;
    private DataFrameTransformCheckpoint lastCheckpoint;
    private DataFrameTransformCheckpoint nextCheckpoint;

    ClientDataFrameIndexerBuilder() {
        this.initialStats = new DataFrameIndexerTransformStats();
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

    ClientDataFrameIndexerBuilder setTransformConfig(DataFrameTransformConfig transformConfig) {
        this.transformConfig = transformConfig;
        return this;
    }

    DataFrameTransformConfig getTransformConfig() {
        return this.transformConfig;
    }

    ClientDataFrameIndexerBuilder setInitialStats(DataFrameIndexerTransformStats initialStats) {
        this.initialStats = initialStats;
        return this;
    }

    ClientDataFrameIndexerBuilder setIndexerState(IndexerState indexerState) {
        this.indexerState = indexerState;
        return this;
    }

    ClientDataFrameIndexerBuilder setInitialPosition(DataFrameIndexerPosition initialPosition) {
        this.initialPosition = initialPosition;
        return this;
    }

    ClientDataFrameIndexerBuilder setProgress(DataFrameTransformProgress progress) {
        this.progress = progress;
        return this;
    }

    ClientDataFrameIndexerBuilder setLastCheckpoint(DataFrameTransformCheckpoint lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
        return this;
    }

    ClientDataFrameIndexerBuilder setNextCheckpoint(DataFrameTransformCheckpoint nextCheckpoint) {
        this.nextCheckpoint = nextCheckpoint;
        return this;
    }
}