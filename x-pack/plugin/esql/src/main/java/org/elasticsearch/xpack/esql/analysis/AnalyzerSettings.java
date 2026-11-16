/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.settings.Setting;

/**
 * Values for cluster level settings used during query analysis.
 */
public record AnalyzerSettings(
    int resultTruncationMaxSize,
    int resultTruncationDefaultSize,
    int timeseriesResultTruncationMaxSize,
    int timeseriesResultTruncationDefaultSize
) {
    /** The value of the row maximum that removes it: a query may then request any number of rows, and memory bounds it. */
    public static final int NO_MAXIMUM = -1;
    /** The largest row maximum the setting takes, and the bound a sort nothing else bounds takes when there is no maximum. */
    public static final int LARGEST_MAXIMUM = 1_000_000;

    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_MAX_SIZE = new Setting<>(
        "esql.query.result_truncation_max_size",
        "10000",
        AnalyzerSettings::parseMaximum,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The row maximum as the setting takes it: a size between one and the largest, or {@link #NO_MAXIMUM} to remove
     * the maximum altogether.
     */
    static int parseMaximum(String value) {
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[esql.query.result_truncation_max_size] must be a number, not [" + value + "]", e);
        }
        if (parsed != NO_MAXIMUM && (parsed < 1 || parsed > LARGEST_MAXIMUM)) {
            throw new IllegalArgumentException(
                "[esql.query.result_truncation_max_size] must be between 1 and "
                    + LARGEST_MAXIMUM
                    + ", or "
                    + NO_MAXIMUM
                    + " for no maximum, not ["
                    + parsed
                    + "]"
            );
        }
        return parsed;
    }

    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_DEFAULT_SIZE = Setting.intSetting(
        "esql.query.result_truncation_default_size",
        1000,
        1,
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE = Setting.intSetting(
        "esql.query.timeseries_result_truncation_default_size",
        10_000,
        1,
        10_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE = Setting.intSetting(
        "esql.query.timeseries_result_truncation_max_size",
        10_000_000,
        1,
        1_000_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
