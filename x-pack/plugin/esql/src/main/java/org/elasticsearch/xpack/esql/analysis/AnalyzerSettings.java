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
    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_MAX_SIZE = Setting.intSetting(
        "esql.query.result_truncation_max_size",
        10000,
        1,
        1000000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
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
        1_000_000,
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
