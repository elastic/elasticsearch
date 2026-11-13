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

    private final ByteSizeValue coordinatingLimit;
    private final ByteSizeValue primaryLimit;
    private final ByteSizeValue replicaLimit;
    private final ByteSizeValue maxOperationSize;
    private final ByteSizeValue splitBulkHighWatermark;
    private final ByteSizeValue splitBulkLowWatermark;
    private final ByteSizeValue indexBufferSize;

    public SettingsIndexingMemoryLimits(Settings settings) {
        this.coordinatingLimit = IndexingPressure.MAX_COORDINATING_BYTES.get(settings);
        this.primaryLimit = IndexingPressure.MAX_PRIMARY_BYTES.get(settings);
        this.replicaLimit = IndexingPressure.MAX_REPLICA_BYTES.get(settings);
        this.maxOperationSize = IndexingPressure.MAX_OPERATION_SIZE.get(settings);
        this.splitBulkHighWatermark = IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.get(settings);
        this.splitBulkLowWatermark = IndexingPressure.SPLIT_BULK_LOW_WATERMARK.get(settings);
        this.indexBufferSize = indexBufferSize(settings);
    }

    private static ByteSizeValue indexBufferSize(Settings settings) {
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
        return indexingBuffer;
    }

    @Override
    public ByteSizeValue coordinatingLimit() {
        return coordinatingLimit;
    }

    @Override
    public ByteSizeValue primaryLimit() {
        return primaryLimit;
    }

    @Override
    public ByteSizeValue replicaLimit() {
        return replicaLimit;
    }

    @Override
    public ByteSizeValue maxOperationSize() {
        return maxOperationSize;
    }

    @Override
    public ByteSizeValue splitBulkHighWatermark() {
        return splitBulkHighWatermark;
    }

    @Override
    public ByteSizeValue splitBulkLowWatermark() {
        return splitBulkLowWatermark;
    }

    @Override
    public ByteSizeValue indexBufferSize() {
        return indexBufferSize;
    }
}
