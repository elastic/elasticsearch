/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;

public class PlanExecutor {

    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final FunctionRegistry functionRegistry;
    private final LogicalPlanOptimizer logicalPlanOptimizer;
    private final Mapper mapper;

    public PlanExecutor(IndexResolver indexResolver) {
        this.indexResolver = indexResolver;
        this.preAnalyzer = new PreAnalyzer();
        this.functionRegistry = new EsqlFunctionRegistry();
        this.logicalPlanOptimizer = new LogicalPlanOptimizer();
        this.mapper = new Mapper(functionRegistry);
    }

    public EsqlSession newSession(EsqlConfiguration cfg) {
        return new EsqlSession(cfg, indexResolver, preAnalyzer, functionRegistry, logicalPlanOptimizer, mapper);
    }
}
