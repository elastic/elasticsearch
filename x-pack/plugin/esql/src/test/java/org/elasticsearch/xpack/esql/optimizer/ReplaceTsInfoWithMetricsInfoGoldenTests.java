/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import java.util.EnumSet;

public class ReplaceTsInfoWithMetricsInfoGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    public void testValuesByMetric() {
        builder("""
            TS k8s
            | TS_INFO
            | WHERE metric_name LIKE "metrics.*"
            | STATS dims = VALUES(dimension_fields) BY metric_name, metric_type
            | SORT metric_name
            | LIMIT 1
            """).stages(STAGES).run();
    }

    public void testStackedFilters() {
        builder("""
            TS k8s
            | TS_INFO
            | WHERE metric_name LIKE "metrics.*"
            | WHERE metric_type == "counter"
            | STATS dims = VALUES(dimension_fields) BY metric_name
            """).stages(STAGES).run();
    }

    public void testCountKeepsTsInfo() {
        builder("""
            TS k8s
            | TS_INFO
            | STATS series = COUNT(*) BY metric_name
            """).stages(STAGES).run();
    }

    public void testDimensionsKeepsTsInfo() {
        builder("""
            TS k8s
            | TS_INFO
            | STATS dims = VALUES(dimensions) BY metric_name
            """).stages(STAGES).run();
    }

    public void testKeepBeforeStats() {
        builder("""
            TS k8s
            | TS_INFO
            | KEEP metric_name, metric_type, dimension_fields
            | WHERE metric_type == "counter"
            | STATS dims = VALUES(dimension_fields) BY metric_name
            """).stages(STAGES).run();
    }

    public void testKeepDimensionsKeepsTsInfo() {
        builder("""
            TS k8s
            | TS_INFO
            | KEEP metric_name, dimensions
            | STATS dims = VALUES(dimensions) BY metric_name
            """).stages(STAGES).run();
    }

    public void testComplexChainBeforeStats() {
        builder("""
            TS k8s
            | TS_INFO
            | WHERE metric_name LIKE "network.*"
            | RENAME metric_name AS name
            | KEEP name, metric_type, unit, dimension_fields, data_stream
            | WHERE unit != "usd"
            | KEEP name, unit, dimension_fields, data_stream
            | STATS dims = VALUES(dimension_fields), units = MAX(unit), streams = COUNT_DISTINCT(data_stream) BY name
            | SORT name
            """).stages(STAGES).run();
    }

    public void testComplexChainReferencingDimensionsKeepsTsInfo() {
        builder("""
            TS k8s
            | TS_INFO
            | WHERE metric_name LIKE "network.*"
            | RENAME metric_name AS name
            | WHERE metric_type == "counter"
            | KEEP name, dimension_fields, dimensions
            | STATS dims = VALUES(dimension_fields), series = MAX(dimensions) BY name
            """).stages(STAGES).run();
    }

    public void testCountDistinctByMetric() {
        builder("""
            TS k8s
            | TS_INFO
            | STATS metrics = COUNT_DISTINCT(metric_name) BY metric_type
            """).stages(STAGES).run();
    }
}
