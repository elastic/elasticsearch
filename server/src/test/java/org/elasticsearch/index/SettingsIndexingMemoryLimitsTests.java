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
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.indices.IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING;
import static org.elasticsearch.indices.IndexingMemoryController.MAX_INDEX_BUFFER_SIZE_SETTING;
import static org.elasticsearch.indices.IndexingMemoryController.MIN_INDEX_BUFFER_SIZE_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class SettingsIndexingMemoryLimitsTests extends ESTestCase {

    public void testLimitsComeFromSettings() {
        var limits = new SettingsIndexingMemoryLimits(
            Settings.builder()
                .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "1mb")
                .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), "2mb")
                .put(IndexingPressure.MAX_REPLICA_BYTES.getKey(), "3mb")
                .put(IndexingPressure.MAX_OPERATION_SIZE.getKey(), "4mb")
                .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "5mb")
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "6mb")
                .put(INDEX_BUFFER_SIZE_SETTING.getKey(), "100mb")
                .build()
        );
        assertThat(limits.coordinatingLimit(), equalTo(ByteSizeValue.ofBytes(1L << 20)));
        assertThat(limits.primaryLimit(), equalTo(ByteSizeValue.ofBytes(2L << 20)));
        assertThat(limits.replicaLimit(), equalTo(ByteSizeValue.ofBytes(3L << 20)));
        assertThat(limits.maxOperationSize(), equalTo(ByteSizeValue.ofBytes(4L << 20)));
        assertThat(limits.splitBulkHighWatermark(), equalTo(ByteSizeValue.ofBytes(5L << 20)));
        assertThat(limits.splitBulkLowWatermark(), equalTo(ByteSizeValue.ofBytes(6L << 20)));
        assertThat(limits.indexBufferSize(), equalTo(ByteSizeValue.ofBytes(100L << 20)));
    }

    public void testIndexBufferClampsApplyToPercentagesOnly() {
        var clamps = Settings.builder()
            .put(MIN_INDEX_BUFFER_SIZE_SETTING.getKey(), "200mb")
            .put(MAX_INDEX_BUFFER_SIZE_SETTING.getKey(), "300mb");

        var belowMin = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "1b").build();
        assertThat(new SettingsIndexingMemoryLimits(belowMin).indexBufferSize(), equalTo(ByteSizeValue.ofBytes(1L)));

        var tinyPercent = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "0.0001%").build();
        assertThat(new SettingsIndexingMemoryLimits(tinyPercent).indexBufferSize(), equalTo(ByteSizeValue.ofBytes(200L << 20)));

        var hugePercent = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "100%").build();
        assertThat(new SettingsIndexingMemoryLimits(hugePercent).indexBufferSize(), equalTo(ByteSizeValue.ofBytes(300L << 20)));
    }
}
