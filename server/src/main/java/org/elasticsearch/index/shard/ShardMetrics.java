/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Node-wide APM instruments that shards record into.
 */
public record ShardMetrics(CodecMetrics codec, MergeMetrics merge, ShardSearchPhaseAPMMetrics search) {

    /**
     * Rollout toggle for {@link CodecMetrics}. Off by default so the codec wrapping layer stays out of the write path until it has soaked
     * in QA. Read once at node startup by {@link #create}; changing it needs a restart.
     */
    public static final Setting<Boolean> CODEC_METRICS_ENABLED = Setting.boolSetting(
        "indices.codec.metrics.enabled",
        false,
        Setting.Property.NodeScope
    );

    public static final ShardMetrics NOOP = new ShardMetrics(
        CodecMetrics.NOOP,
        MergeMetrics.NOOP,
        new ShardSearchPhaseAPMMetrics(TelemetryProvider.NOOP.getMeterRegistry())
    );

    public static ShardMetrics create(MeterRegistry meterRegistry, Settings settings) {
        return new ShardMetrics(
            CODEC_METRICS_ENABLED.get(settings) ? new CodecMetrics(meterRegistry) : CodecMetrics.NOOP,
            new MergeMetrics(meterRegistry),
            new ShardSearchPhaseAPMMetrics(meterRegistry)
        );
    }
}
