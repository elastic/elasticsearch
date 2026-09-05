/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.index.engine.MergeMetrics;
import org.elasticsearch.index.search.stats.ShardSearchPhaseAPMMetrics;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;

/**
 * Node-wide APM instruments that shards record into.
 */
public record ShardMetrics(CodecMetrics codec, MergeMetrics merge, ShardSearchPhaseAPMMetrics search) {

    /**
     * Rollout toggle for {@link CodecMetrics}. Off by default so the codec wrapping layer stays out of the write path until it has soaked
     * in QA; static because the wrapper is chosen once per shard when the engine is created.
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

    /**
     * The {@code error_type} attribute value for a failure: the simple class name of the corruption exception if one is anywhere in
     * the cause or suppressed chain, otherwise of the innermost non-wrapper cause.
     */
    public static String errorType(Throwable t) {
        IOException corruption = ExceptionsHelper.unwrapCorruption(t);
        Throwable cause = corruption != null ? corruption : ExceptionsHelper.unwrapCause(t);
        return cause.getClass().getSimpleName();
    }
}
