/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.indices.IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING;
import static org.elasticsearch.indices.IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING;
import static org.elasticsearch.indices.IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING;

/**
 * The {@link IndexingMemoryLimits} configured explicitly by node settings.
 */
public final class SettingsIndexingMemoryLimits implements IndexingMemoryLimits {

    private final long coordinatingLimit;
    private final long primaryLimit;
    private final long replicaLimit;
    private final long maxOperationSize;
    private final long splitBulkHighWatermark;
    private final long splitBulkLowWatermark;
    private final long indexBuffer;

    public SettingsIndexingMemoryLimits(Settings settings) {
        this.coordinatingLimit = IndexingPressure.MAX_COORDINATING_BYTES.get(settings).getBytes();
        this.primaryLimit = IndexingPressure.MAX_PRIMARY_BYTES.get(settings).getBytes();
        this.replicaLimit = IndexingPressure.MAX_REPLICA_BYTES.get(settings).getBytes();
        this.maxOperationSize = IndexingPressure.MAX_OPERATION_SIZE.get(settings).getBytes();
        this.splitBulkHighWatermark = IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.get(settings).getBytes();
        this.splitBulkLowWatermark = IndexingPressure.SPLIT_BULK_LOW_WATERMARK.get(settings).getBytes();
        this.indexBuffer = indexBuffer(settings);
    }

    private static long indexBuffer(Settings settings) {
        ByteSizeValue indexingBuffer = INDEX_BUFFER_SIZE_SETTING.get(settings);

        String indexingBufferSetting = settings.get(INDEX_BUFFER_SIZE_SETTING.getKey());
        // null means we used the default (10%)
        if (indexingBufferSetting == null || indexingBufferSetting.endsWith("%")) {
            // We only apply the min/max when % value was used for the index buffer:
            ByteSizeValue minIndexingBuffer = MIN_INDEX_BUFFER_SIZE_SETTING.get(settings);
            ByteSizeValue maxIndexingBuffer = MAX_INDEX_BUFFER_SIZE_SETTING.get(settings);
            if (indexingBuffer.getBytes() < minIndexingBuffer.getBytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer.getBytes() != -1 && indexingBuffer.getBytes() > maxIndexingBuffer.getBytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        }
        return indexingBuffer.getBytes();
    }

    @Override
    public long coordinatingLimitBytes() {
        return coordinatingLimit;
    }

    @Override
    public long primaryLimitBytes() {
        return primaryLimit;
    }

    @Override
    public long replicaLimitBytes() {
        return replicaLimit;
    }

    @Override
    public long maxOperationSizeBytes() {
        return maxOperationSize;
    }

    @Override
    public long splitBulkHighWatermarkBytes() {
        return splitBulkHighWatermark;
    }

    @Override
    public long splitBulkLowWatermarkBytes() {
        return splitBulkLowWatermark;
    }

    @Override
    public long indexBufferBytes() {
        return indexBuffer;
    }
}
