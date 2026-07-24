/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;

/**
 * Analyzer behavior for time-series and k8s type-conflict scenarios with unmapped fields.
 */
public class AnalyzerUnmappedTimeSeriesGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    private static final String COMPACT_MULTI_TYPE_ES_FIELD = "compact_multi_type_es_field";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedTimeSeriesGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testTimeSeriesRateUnmappedNullify() throws Exception {
        builder(nullify("""
            TS k8s
            | STATS r = RATE(does_not_exist) BY tbucket(1 hour)
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTimeSeriesFirstOverTimeUnmappedNullify() throws Exception {
        builder(nullify("""
            TS k8s
            | STATS f = FIRST_OVER_TIME(does_not_exist::DOUBLE) BY tbucket(1 hour)
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTimeSeriesFirstOverTimeUnmappedLoad() throws Exception {
        builder(load("""
            TS k8s
            | STATS f = FIRST_OVER_TIME(does_not_exist::DOUBLE) BY tbucket(1 hour)
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTypeConflictTimeseriesLongUnmappedWithCastNullify() throws Exception {
        builder(nullify("""
            FROM k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """)).run();
    }

    public void testTypeConflictTimeseriesLongUnmappedWithCastLoad() throws Exception {
        builder(load("""
            FROM k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """)).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testTSTypeConflictTimeseriesLongUnmappedWithCastNullify() throws Exception {
        builder(nullify("""
            TS k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTSTypeConflictTimeseriesLongUnmappedWithCastLoad() throws Exception {
        builder(load("""
            TS k8s, k8s_unmapped
            | EVAL bytes = network.bytes_in::long
            | KEEP bytes
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testTypeConflictTimeseriesDoubleUnmappedWithCastNullify() throws Exception {
        builder(nullify("""
            FROM k8s, k8s_unmapped
            | EVAL cost = network.cost::double
            | KEEP cost
            """)).run();
    }

    public void testTypeConflictTimeseriesDoubleUnmappedWithCastLoad() throws Exception {
        builder(load("""
            FROM k8s, k8s_unmapped
            | EVAL cost = network.cost::double
            | KEEP cost
            """)).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testTypeConflictTimeseriesStatsWithCastNullify() throws Exception {
        builder(nullify("""
            FROM k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """)).run();
    }

    public void testTypeConflictTimeseriesStatsWithCastLoad() throws Exception {
        builder(load("""
            FROM k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """)).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testTSTypeConflictTimeseriesStatsWithCastNullify() throws Exception {
        builder(nullify("""
            TS k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTSTypeConflictTimeseriesStatsWithCastLoad() throws Exception {
        builder(load("""
            TS k8s, k8s_unmapped
            | STATS s = SUM(network.bytes_in::long) BY cluster
            """)).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }

    public void testTypeConflictTimeseriesWhereWithCastNullify() throws Exception {
        builder(nullify("""
            FROM k8s, k8s_unmapped
            | WHERE network.cost::double > 10.0
            | KEEP cluster, network.cost
            """)).run();
    }

    public void testTypeConflictTimeseriesWhereWithCastLoad() throws Exception {
        builder(load("""
            FROM k8s, k8s_unmapped
            | WHERE network.cost::double > 10.0
            | KEEP cluster, network.cost
            """)).expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD).run();
    }
}
