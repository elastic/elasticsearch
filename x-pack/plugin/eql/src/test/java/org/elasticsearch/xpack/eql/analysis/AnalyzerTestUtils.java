/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;

import static org.elasticsearch.xpack.eql.EqlTestUtils.TEST_CFG;

public final class AnalyzerTestUtils {

    private AnalyzerTestUtils() {}

    public static Analyzer analyzer() {
        return new Analyzer(new AnalyzerContext(TEST_CFG, new EqlFunctionRegistry()), new Verifier(new Metrics()));
    }

    public static Analyzer analyzer(Verifier verifier) {
        return analyzer(TEST_CFG, new EqlFunctionRegistry(), verifier);
    }

    public static Analyzer analyzer(EqlConfiguration configuration) {
        return analyzer(configuration, new EqlFunctionRegistry());
    }

    public static Analyzer analyzer(EqlConfiguration configuration, FunctionRegistry registry) {
        return analyzer(configuration, registry, new Verifier(new Metrics()));
    }

    public static Analyzer analyzer(EqlConfiguration configuration, FunctionRegistry registry, Verifier verifier) {
        return new Analyzer(new AnalyzerContext(configuration, registry), verifier);
    }
}
