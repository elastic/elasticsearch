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

    /**
     * How long a sampled query is kept, counted from when it was picked. The sample is meant to follow the current
     * traffic, so what is older than this is of no use and is deleted.
     */
    public static final Setting<TimeValue> RETENTION = Setting.timeSetting(
        "xpack.query_sampling.retention",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * The number of searches per hour that should be captured on a node even if that takes a higher rate than
     * {@code capture_rate}, so that a node with little traffic still contributes to the sample. A node whose traffic
     * is high enough captures more than that at the configured rate and is not affected. 0 means no floor.
     */
    public static final Setting<Long> MIN_CAPTURES_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.min_captures_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The share of the time that live kNN searches take that exact searches may take, which is how much ground truth
     * a node computes by itself. Exact searches scan the whole index, so this keeps them from competing with the
     * searches they measure. 0 means that nothing is computed unless it is asked for.
     */
    public static final Setting<Double> SAMPLING_COST_RATIO = Setting.doubleSetting(
        "xpack.query_sampling.sampling_cost_ratio",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The most queries that a node picks per hour, apart from the hottest ones, which are always picked. It keeps the
     * sample from growing without limit when many different queries are searched. 0 means there is no limit.
     */
    public static final Setting<Long> MAX_PICKS_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.max_picks_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The rate of picks per hour that the acceptance scale is steered towards, apart from the picks of the hottest queries.
     * The setting of the scale is what it starts from, and it is multiplied with what is needed to get to this rate. 0
     * leaves the scale as it is set. Unlike {@link #MAX_PICKS_PER_HOUR} it is not a limit: it only aims for a rate.
     */
    public static final Setting<Long> TARGET_PICKS_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.target_picks_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * γ, the scale of the probability with which a query is picked: how likely a query seen for the first time is
     * picked (about 0.69·γ), and how fast that falls as the query is searched more. A bigger value samples more
     * queries, and more of the popular ones.
     */
    public static final Setting<Double> ACCEPTANCE_SCALE = Setting.doubleSetting(
        "xpack.query_sampling.acceptance_scale",
        1.0,
        0.0,
        100.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Estimated number of searches from which a query is always picked. The hottest queries carry a large part of
     * the traffic, leaving them to chance would make the estimates swing on one coin flip. A value of 1 picks every
     * query that is captured.
     */
    public static final Setting<Long> HEAD_THRESHOLD = Setting.longSetting(
        "xpack.query_sampling.head_threshold",
        100L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How long a query is remembered after its last search. Its count, and so how likely it is to be picked, only
     * covers the searches within this time. A query that is not seen for between one and two windows is forgotten.
     */
    public static final Setting<TimeValue> MULTIPLICITY_WINDOW = Setting.timeSetting(
        "xpack.query_sampling.multiplicity_window",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private QuerySamplingSettings() {}

    public static List<Setting<?>> getSettings() {
        return List.of(
            ENABLED,
            CAPTURE_RATE,
            MIN_CAPTURES_PER_HOUR,
            SAMPLING_COST_RATIO,
            MAX_PICKS_PER_HOUR,
            TARGET_PICKS_PER_HOUR,
            ACCEPTANCE_SCALE,
            HEAD_THRESHOLD,
            MULTIPLICITY_WINDOW,
            WEIGHTS_REFRESH_INTERVAL,
            RETENTION
        );
    }
}
