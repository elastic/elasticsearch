/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.stats.Metrics;

import static org.elasticsearch.xpack.sql.SqlTestUtils.TEST_CFG;

public final class AnalyzerTestUtils {

    private AnalyzerTestUtils() {}

    public static Analyzer analyzer(IndexResolution resolution) {
        return analyzer(TEST_CFG, new SqlFunctionRegistry(), resolution);
    }

    public static Analyzer analyzer(IndexResolution resolution, Verifier verifier) {
        return analyzer(TEST_CFG, new SqlFunctionRegistry(), resolution, verifier);
    }

    public static Analyzer analyzer(SqlConfiguration configuration, IndexResolution resolution) {
        return analyzer(configuration, new SqlFunctionRegistry(), resolution);
    }

    public static Analyzer analyzer(SqlConfiguration configuration, FunctionRegistry registry, IndexResolution resolution) {
        return analyzer(configuration, registry, resolution, new Verifier(new Metrics()));
    }

    public static Analyzer analyzer(
        SqlConfiguration configuration,
        FunctionRegistry registry,
        IndexResolution resolution,
        Verifier verifier
    ) {
        return new Analyzer(new AnalyzerContext(configuration, registry, resolution), verifier);
    }
}
