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

    /**
     * Probability that an eligible kNN search is captured; each search is an independent coin flip. A node with little traffic
     * captures more, see {@link #MIN_CAPTURES_PER_HOUR}.
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
     * Per-node floor of captures per hour, raising the rate above {@code capture_rate} for low-traffic nodes, so that a quiet
     * node or hour still gives samples. 0 means no floor. With a floor, a {@code capture_rate} of 0 does not stop captures:
     * switch sampling off with {@code xpack.query_sampling.enabled} for that.
     */
    public static final Setting<Long> MIN_CAPTURES_PER_HOUR = Setting.longSetting(
        "xpack.query_sampling.min_captures_per_hour",
        100L,
        0L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Most queries a node picks per hour, apart from the hottest ones; once it is reached the chance of a pick is cut to a
     * hundredth, not to 0, so a few more may pass. 0 means no limit.
     */
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
     * disables. Built per node, in memory: before the clusters are fitted, ten vectors per cluster wait for it, for each
     * field, which is why the number is limited.
     */
    public static final Setting<Integer> SPATIAL_CLUSTERS = Setting.intSetting(
        "xpack.query_sampling.spatial_clusters",
        0,
        0,
        64,
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
     * γ, the scale of the pick probability: the first captured arrival of a query is picked with
     * {@code min(1, γ·ln(1 + 1/capture_rate))}, about 0.69·γ when every search is captured, falling as it is searched more.
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
     * Captured arrivals from which a query is always picked, so that estimates do not hinge on one coin flip. Head
     * picks are not limited by {@link #MAX_PICKS_PER_HOUR} or {@link #TARGET_PICKS_PER_HOUR}. 1 picks every captured query.
     */
    public static final Setting<Long> HEAD_THRESHOLD = Setting.longSetting(
        "xpack.query_sampling.head_threshold",
        100L,
        1L,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The window that the arrivals of a query are counted in. A query is forgotten between one and two windows after its
     * last search, and then its count starts again.
     */
    public static final Setting<TimeValue> MULTIPLICITY_WINDOW = Setting.timeSetting(
        "xpack.query_sampling.multiplicity_window",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private QuerySamplingSettings() {}

    public static List<Setting<?>> getSettings() {
        return List.of(
            ENABLED,
            CAPTURE_RATE,
            MIN_CAPTURES_PER_HOUR,
            MAX_PICKS_PER_HOUR,
            TARGET_PICKS_PER_HOUR,
            SPATIAL_CLUSTERS,
            SPATIAL_BALANCE,
            HARDNESS_TILT,
            ACCEPTANCE_SCALE,
            HEAD_THRESHOLD,
            MULTIPLICITY_WINDOW
        );
    }
}
