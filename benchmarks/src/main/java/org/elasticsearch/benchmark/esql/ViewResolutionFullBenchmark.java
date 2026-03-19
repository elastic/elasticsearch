/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.openjdk.jmh.annotations.Param;

/**
 * Full view resolution benchmark covering all scenario/filter combinations.
 * Intended for local exploration runs, not nightly CI.
 * <p>
 * Run with:
 * <pre>
 * ./gradlew :benchmarks:run --args='ViewResolutionFullBenchmark'
 * </pre>
 */
public class ViewResolutionFullBenchmark extends ViewResolutionBenchmarkBase {

    @Param(
        {
            "disabled",
            "enabled_no_views",
            "enabled_index",
            "depth_1",
            "depth_1_filter",
            "depth_2",
            "depth_2_filter",
            "depth_3",
            "depth_3_filter",
            "depth_4",
            "depth_4_filter",
            "depth_5",
            "depth_5_filter" }
    )
    String scenario;

    @Param({ "false", "true" })
    String queryFilter;

    @Override
    protected String getScenario() {
        return scenario;
    }

    @Override
    protected String getQueryFilter() {
        return queryFilter;
    }
}
