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
        assertThat(limits.coordinatingLimitBytes(), equalTo(1L << 20));
        assertThat(limits.primaryLimitBytes(), equalTo(2L << 20));
        assertThat(limits.replicaLimitBytes(), equalTo(3L << 20));
        assertThat(limits.maxOperationSizeBytes(), equalTo(4L << 20));
        assertThat(limits.splitBulkHighWatermarkBytes(), equalTo(5L << 20));
        assertThat(limits.splitBulkLowWatermarkBytes(), equalTo(6L << 20));
        assertThat(limits.indexBufferBytes(), equalTo(100L << 20));
    }

    public void testIndexBufferClampsApplyToPercentagesOnly() {
        var clamps = Settings.builder()
            .put(MIN_INDEX_BUFFER_SIZE_SETTING.getKey(), "200mb")
            .put(MAX_INDEX_BUFFER_SIZE_SETTING.getKey(), "300mb");

        var belowMin = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "1b").build();
        assertThat(new SettingsIndexingMemoryLimits(belowMin).indexBufferBytes(), equalTo(1L));

        var tinyPercent = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "0.0001%").build();
        assertThat(new SettingsIndexingMemoryLimits(tinyPercent).indexBufferBytes(), equalTo(200L << 20));

        var hugePercent = Settings.builder().put(clamps.build()).put(INDEX_BUFFER_SIZE_SETTING.getKey(), "100%").build();
        assertThat(new SettingsIndexingMemoryLimits(hugePercent).indexBufferBytes(), equalTo(300L << 20));
    }
}
