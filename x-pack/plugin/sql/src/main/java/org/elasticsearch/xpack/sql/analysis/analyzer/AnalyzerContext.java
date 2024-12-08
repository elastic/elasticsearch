/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;

import java.util.function.Function;

public record AnalyzerContext(
    SqlConfiguration configuration,
    FunctionRegistry functionRegistry,
    IndexResolution indexResolution,
    Holder<Function<LogicalPlan, LogicalPlan>> analyzeWithoutVerify
) {
    public AnalyzerContext(SqlConfiguration configuration, FunctionRegistry functionRegistry, IndexResolution indexResolution) {
        this(configuration, functionRegistry, indexResolution, new Holder<>());
    }
}
