/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Settings of the query sampling pipeline. What decides how much is sampled is dynamic, so that sampling can be
 * switched on, tuned or switched off without a restart.
 */
public final class QuerySamplingSettings {

    /**
     * Master switch. When {@code false} the pipeline must not do any work on the search path.
     */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "xpack.query_sampling.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Probability that an eligible kNN search is captured. Each search is an independent coin flip, so
     * the captured stream is a uniform sample of the traffic regardless of how requests arrive in time.
     */
    public static final Setting<Double> CAPTURE_RATE = Setting.doubleSetting(
        "xpack.query_sampling.capture_rate",
        0.01,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often the weights stored with the sampled queries are brought up to date. It only matters for how
     * stale they can be, and is read when the node starts.
     */
    public static final Setting<TimeValue> WEIGHTS_REFRESH_INTERVAL = Setting.timeSetting(
        "xpack.query_sampling.weights_refresh_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private QuerySamplingSettings() {}

    public static List<Setting<?>> getSettings() {
        return List.of(ENABLED, CAPTURE_RATE, WEIGHTS_REFRESH_INTERVAL);
    }
}
