/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexingPressure;

/**
 * Settings-derived implementation of {@link IndexingMemoryLimits}, preserving the existing
 * {@link IndexingPressure} and {@link IndexingMemoryController} behaviour for stateful clusters.
 */
class SettingsBasedIndexingMemoryLimits implements IndexingMemoryLimits {

    private final long coordinatingLimitBytes;
    private final long primaryLimitBytes;
    private final long replicaLimitBytes;
    private final long operationLimitBytes;
    private final long indexBufferBytes;

    SettingsBasedIndexingMemoryLimits(Settings settings) {
        this.coordinatingLimitBytes = IndexingPressure.MAX_COORDINATING_BYTES.get(settings).getBytes();
        this.primaryLimitBytes = IndexingPressure.MAX_PRIMARY_BYTES.get(settings).getBytes();
        this.replicaLimitBytes = IndexingPressure.MAX_REPLICA_BYTES.get(settings).getBytes();
        this.operationLimitBytes = IndexingPressure.MAX_OPERATION_SIZE.get(settings).getBytes();
        this.indexBufferBytes = resolveIndexBufferBytes(settings);
    }

    private static long resolveIndexBufferBytes(Settings settings) {
        ByteSizeValue indexingBuffer = IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.get(settings);
        String indexingBufferSetting = settings.get(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.getKey());
        if (indexingBufferSetting == null || indexingBufferSetting.endsWith("%")) {
            ByteSizeValue min = IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING.get(settings);
            ByteSizeValue max = IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING.get(settings);
            if (indexingBuffer.getBytes() < min.getBytes()) {
                return min.getBytes();
            }
            if (max.getBytes() != -1 && indexingBuffer.getBytes() > max.getBytes()) {
                return max.getBytes();
            }
        }
        return indexingBuffer.getBytes();
    }

    @Override
    public long coordinatingLimitBytes() {
        return coordinatingLimitBytes;
    }

    @Override
    public long primaryLimitBytes() {
        return primaryLimitBytes;
    }

    @Override
    public long replicaLimitBytes() {
        return replicaLimitBytes;
    }

    @Override
    public long operationLimitBytes() {
        return operationLimitBytes;
    }

    @Override
    public long indexBufferBytes() {
        return indexBufferBytes;
    }
}
