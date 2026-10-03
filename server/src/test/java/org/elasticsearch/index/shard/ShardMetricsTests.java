/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

public class ShardMetricsTests extends ESTestCase {

    public void testCodecMetricsAreNoopUnlessEnabled() {
        ShardMetrics defaults = ShardMetrics.create(new RecordingMeterRegistry(), Settings.EMPTY);
        assertSame(CodecMetrics.NOOP, defaults.codec());

        boolean enabled = randomBoolean();
        Settings settings = Settings.builder().put(ShardMetrics.CODEC_METRICS_ENABLED.getKey(), enabled).build();
        ShardMetrics metrics = ShardMetrics.create(new RecordingMeterRegistry(), settings);
        assertEquals(enabled, metrics.codec() != CodecMetrics.NOOP);
    }
}
