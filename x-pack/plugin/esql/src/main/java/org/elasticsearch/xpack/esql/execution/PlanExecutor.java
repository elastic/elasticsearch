/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.QueryBuilderResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.PlanningMetrics;
import org.elasticsearch.xpack.esql.stats.PlanningMetricsManager;
import org.elasticsearch.xpack.esql.stats.QueryMetric;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {

    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final EsqlFunctionRegistry functionRegistry;
    private final Mapper mapper;
    private final Metrics metrics;
    private final Verifier verifier;
    private final PlanningMetricsManager planningMetricsManager;

    public PlanExecutor(IndexResolver indexResolver, MeterRegistry meterRegistry, XPackLicenseState licenseState) {
        this.indexResolver = indexResolver;
        this.preAnalyzer = new PreAnalyzer();
        this.functionRegistry = new EsqlFunctionRegistry();
        this.mapper = new Mapper();
        this.metrics = new Metrics(functionRegistry);
        this.verifier = new Verifier(metrics, licenseState);
        this.planningMetricsManager = new PlanningMetricsManager(meterRegistry);
    }

    public void esql(
        EsqlQueryRequest request,
        String sessionId,
        Configuration cfg,
        EnrichPolicyResolver enrichPolicyResolver,
        EsqlExecutionInfo executionInfo,
        IndicesExpressionGrouper indicesExpressionGrouper,
        EsqlSession.PlanRunner planRunner,
        QueryBuilderResolver queryBuilderResolver,
        ActionListener<Result> listener
    ) {
        final PlanningMetrics planningMetrics = new PlanningMetrics();
        final var session = new EsqlSession(
            sessionId,
            cfg,
            indexResolver,
            enrichPolicyResolver,
            preAnalyzer,
            functionRegistry,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(cfg)),
            mapper,
            verifier,
            planningMetrics,
            indicesExpressionGrouper,
            queryBuilderResolver
        );
        QueryMetric clientId = QueryMetric.fromString("rest");
        metrics.total(clientId);
        session.execute(request, executionInfo, planRunner, wrap(x -> {
            planningMetricsManager.publish(planningMetrics, true);
            listener.onResponse(x);
        }, ex -> {
            // TODO when we decide if we will differentiate Kibana from REST, this String value will likely come from the request
            metrics.failed(clientId);
            planningMetricsManager.publish(planningMetrics, false);
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
