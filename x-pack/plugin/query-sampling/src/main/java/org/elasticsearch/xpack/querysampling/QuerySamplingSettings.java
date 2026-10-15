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

/** Settings of the query sampling pipeline; the sampling knobs are dynamic so it can be tuned without a restart. */
public final class QuerySamplingSettings {

    /** When false the pipeline does no work on the search path. */
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "xpack.query_sampling.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Probability that an eligible kNN search is captured; each search is an independent coin flip. */
    public static final Setting<Double> CAPTURE_RATE = Setting.doubleSetting(
        "xpack.query_sampling.capture_rate",
        0.01,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> WEIGHTS_REFRESH_INTERVAL = Setting.timeSetting(
        "xpack.query_sampling.weights_refresh_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /** How long a sampled query is kept, counted from when it was picked. */
    public static final Setting<TimeValue> RETENTION = Setting.timeSetting(
        "xpack.query_sampling.retention",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    /**
     * Per-node floor of captures per hour, raising the rate above {@code capture_rate} for low-traffic nodes. 0
     * means no floor.
     */
    public static final Setting<Long> MIN_CAPTURES_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.min_captures_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Share of live kNN search time that exact (ground truth) searches may take. 0 computes nothing unless asked
     * for.
     */
    public static final Setting<Double> SAMPLING_COST_RATIO = Setting.doubleSetting(
        "xpack.query_sampling.sampling_cost_ratio",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Most queries a node picks per hour, apart from the hottest ones. 0 means no limit. */
    public static final Setting<Long> MAX_PICKS_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.max_picks_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Picks per hour (apart from the hottest queries) that the acceptance scale is steered towards; unlike the
     * max it is only a target. 0 leaves the scale as set.
     */
    public static final Setting<Long> TARGET_PICKS_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.target_picks_per_hour",
        0L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Number of clusters of query vectors per field used to balance the sample, see {@link #SPATIAL_BALANCE}. 0
     * disables. Built per node, in memory.
     */
    public static final Setting<Integer> SPATIAL_CLUSTERS = Setting.intSetting(
        "xpack.query_sampling.spatial_clusters",
        0,
        0,
        256,
        Setting.Property.NodeScope
    );

    /**
     * 0 follows the traffic, 1 picks as many queries from each cluster. No effect without {@link
     * #SPATIAL_CLUSTERS}.
     */
    public static final Setting<Double> SPATIAL_BALANCE = Setting.doubleSetting(
        "xpack.query_sampling.spatial_balance",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Tilts picking towards hard queries (hits all about as good as the first) by {@code e^η} for the hardest
     * third and {@code e^-η} for the easiest. 0 does not tilt.
     */
    public static final Setting<Double> HARDNESS_TILT = Setting.doubleSetting(
        "xpack.query_sampling.hardness_tilt",
        0.0,
        0.0,
        10.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Share of captured searches also kept as events (a uniform sample of arrivals, unlike the once-per-query
     * sample). 0 keeps none.
     */
    public static final Setting<Double> EVENT_SLICE_RATE = Setting.doubleSetting(
        "xpack.query_sampling.event_slice_rate",
        0.0,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Whether to estimate filter selectivity; costs two no-hit searches per new filtered query, so off by default. */
    public static final Setting<Boolean> ESTIMATE_SELECTIVITY = Setting.boolSetting(
        "xpack.query_sampling.estimate_selectivity",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often the elected master grows the golden dataset with new queries that have ground truth. 0 only
     * promotes when asked.
     */
    public static final Setting<TimeValue> GOLDEN_PROMOTION_INTERVAL = Setting.timeSetting(
        "xpack.query_sampling.golden_promotion_interval",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Cap on a self-grown golden version; the longest-ago picked queries are dropped. Versions made on request
     * are not capped.
     */
    public static final Setting<Integer> GOLDEN_MAX_QUERIES = Setting.intSetting(
        "xpack.query_sampling.golden_max_queries",
        1000,
        1,
        10_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * γ, the scale of the pick probability: a first-seen query is picked with about 0.69·γ, falling as it is
     * searched more.
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
     * Estimated search count from which a query is always picked, so that estimates do not hinge on one coin
     * flip. 1 picks every captured query.
     */
    public static final Setting<Long> HEAD_THRESHOLD = Setting.longSetting(
        "xpack.query_sampling.head_threshold",
        100L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** How long a query is remembered after its last search; its count only covers this window. */
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
            SPATIAL_CLUSTERS,
            SPATIAL_BALANCE,
            HARDNESS_TILT,
            EVENT_SLICE_RATE,
            ESTIMATE_SELECTIVITY,
            GOLDEN_PROMOTION_INTERVAL,
            GOLDEN_MAX_QUERIES,
            ACCEPTANCE_SCALE,
            HEAD_THRESHOLD,
            MULTIPLICITY_WINDOW,
            WEIGHTS_REFRESH_INTERVAL,
            RETENTION
        );
    }
}
