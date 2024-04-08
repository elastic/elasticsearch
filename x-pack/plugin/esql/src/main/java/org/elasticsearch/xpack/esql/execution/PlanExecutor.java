/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.session.EsqlIndexResolver;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.QueryMetric;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {

    private final IndexResolver indexResolver;
    private final EsqlIndexResolver esqlIndexResolver;
    private final PreAnalyzer preAnalyzer;
    private final FunctionRegistry functionRegistry;
    private final Mapper mapper;
    private final Metrics metrics;
    private final Verifier verifier;

    public PlanExecutor(IndexResolver indexResolver, EsqlIndexResolver esqlIndexResolver) {
        this.indexResolver = indexResolver;
        this.esqlIndexResolver = esqlIndexResolver;
        this.preAnalyzer = new PreAnalyzer();
        this.functionRegistry = new EsqlFunctionRegistry();
        this.mapper = new Mapper(functionRegistry);
        this.metrics = new Metrics();
        this.verifier = new Verifier(metrics);
    }

    public void esql(
        EsqlQueryRequest request,
        String sessionId,
        EsqlConfiguration cfg,
        EnrichPolicyResolver enrichPolicyResolver,
        ActionListener<PhysicalPlan> listener
    ) {
        final var session = new EsqlSession(
            sessionId,
            cfg,
            indexResolver,
            esqlIndexResolver,
            enrichPolicyResolver,
            preAnalyzer,
            functionRegistry,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(cfg)),
            mapper,
            verifier
        );
        QueryMetric clientId = QueryMetric.fromString("rest");
        metrics.total(clientId);
        session.execute(request, wrap(listener::onResponse, ex -> {
            // TODO when we decide if we will differentiate Kibana from REST, this String value will likely come from the request
            metrics.failed(clientId);
            listener.onFailure(ex);
        }));
    }

    public IndexResolver indexResolver() {
        return indexResolver;
    }

    public Metrics metrics() {
        return this.metrics;
    }
}
