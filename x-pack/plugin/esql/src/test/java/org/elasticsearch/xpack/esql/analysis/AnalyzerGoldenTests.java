/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Analyzer golden tests backed by {@link org.elasticsearch.xpack.esql.CsvTests#testDatasets} CSV datasets
 * (e.g. union-typed {@code sample_data} / {@code sample_data_ts_nanos}).
 */
public class AnalyzerGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> ANALYSIS_ONLY = EnumSet.of(Stage.ANALYSIS);

    /**
     * Explicit {@code ::long} on {@code @timestamp} must compose with implicit millis→nanos unification:
     * {@code TOLONG} wraps {@code TO_DATE_NANOS}, not the raw field.
     */
    public void testUnionTimestampExplicitLongComposesWithImplicitNanos() throws Exception {
        runGoldenTest("""
            FROM sample_data, sample_data_ts_nanos
            | EVAL ts = @timestamp::long
            """, ANALYSIS_ONLY);
    }
}
