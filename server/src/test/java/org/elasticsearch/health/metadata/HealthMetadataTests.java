/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class HealthMetadataTests extends ESTestCase {

    public void testFreeBytesCalculationOfAbsoluteValue() {
        HealthMetadata.Disk metadata = HealthMetadata.Disk.newBuilder()
            .highWatermark("100B", "bytes-high")
            .floodStageWatermark("50B", "bytes-flood")
            .frozenFloodStageWatermark("50B", "bytes-frozen-flood")
            .frozenFloodStageMaxHeadroom("20B", "headroom")
            .build();
        assertThat(metadata.getFreeBytesHighWatermark(ByteSizeValue.MINUS_ONE), equalTo(ByteSizeValue.ofBytes(100)));
        assertThat(metadata.getFreeBytesFloodStageWatermark(ByteSizeValue.MINUS_ONE), equalTo(ByteSizeValue.ofBytes(50)));
        assertThat(metadata.getFreeBytesFrozenFloodStageWatermark(ByteSizeValue.MINUS_ONE), equalTo(ByteSizeValue.ofBytes(50)));
    }

    public void testFreeBytesCalculationMaxHeadroom() {
        HealthMetadata.Disk metadata = HealthMetadata.Disk.newBuilder()
            .highWatermark("90%", "ratio-high")
            .floodStageWatermark("95%", "ratio-flood")
            .frozenFloodStageWatermark("95%", "ratio-frozen-flood")
            .frozenFloodStageMaxHeadroom("20B", "headroom")
            .build();
        // For now only the frozen tier is using the max headroom setting
        assertThat(metadata.getFreeBytesFrozenFloodStageWatermark(ByteSizeValue.ofBytes(1000)), equalTo(ByteSizeValue.ofBytes(20)));
    }

    public void testFreeBytesCalculationPercent() {
        HealthMetadata.Disk metadata = HealthMetadata.Disk.newBuilder()
            .highWatermark("90%", "ratio-high")
            .floodStageWatermark("95%", "ratio-flood")
            .frozenFloodStageWatermark("95%", "ratio-frozen-flood")
            .frozenFloodStageMaxHeadroom("60B", "headroom")
            .build();
        assertThat(metadata.getFreeBytesHighWatermark(ByteSizeValue.ofBytes(1000)), equalTo(ByteSizeValue.ofBytes(100)));
        assertThat(metadata.getFreeBytesFloodStageWatermark(ByteSizeValue.ofBytes(1000)), equalTo(ByteSizeValue.ofBytes(50)));
        assertThat(metadata.getFreeBytesFrozenFloodStageWatermark(ByteSizeValue.ofBytes(1000)), equalTo(ByteSizeValue.ofBytes(50)));
    }
}
