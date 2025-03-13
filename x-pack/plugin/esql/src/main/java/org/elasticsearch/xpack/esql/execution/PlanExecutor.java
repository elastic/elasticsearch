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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.elasticsearch.xpack.esql.telemetry.QueryMetric;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {

    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final EsqlFunctionRegistry functionRegistry;
    private final Mapper mapper;
    private final Metrics metrics;
    private final Verifier verifier;
    private final PlanTelemetryManager planTelemetryManager;

    public PlanExecutor(IndexResolver indexResolver, MeterRegistry meterRegistry, XPackLicenseState licenseState) {
        this.indexResolver = indexResolver;
        this.preAnalyzer = new PreAnalyzer();
        this.functionRegistry = new EsqlFunctionRegistry();
        this.mapper = new Mapper();
        this.metrics = new Metrics(functionRegistry);
        this.verifier = new Verifier(metrics, licenseState);
        this.planTelemetryManager = new PlanTelemetryManager(meterRegistry);
    }

    public void esql(
        EsqlQueryRequest request,
        String sessionId,
        Configuration cfg,
        FoldContext foldContext,
        EnrichPolicyResolver enrichPolicyResolver,
        EsqlExecutionInfo executionInfo,
        IndicesExpressionGrouper indicesExpressionGrouper,
        EsqlSession.PlanRunner planRunner,
        TransportActionServices services,
        ActionListener<Result> listener
    ) {
        final PlanTelemetry planTelemetry = new PlanTelemetry(functionRegistry);
        final var session = new EsqlSession(
            sessionId,
            cfg,
            indexResolver,
            enrichPolicyResolver,
            preAnalyzer,
            functionRegistry,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(cfg, foldContext)),
            mapper,
            verifier,
            planTelemetry,
            indicesExpressionGrouper,
            services
        );
        QueryMetric clientId = QueryMetric.fromString("rest");
        metrics.total(clientId);

        ActionListener<Result> executeListener = wrap(x -> {
            planTelemetryManager.publish(planTelemetry, true);
            listener.onResponse(x);
        }, ex -> {
            // TODO when we decide if we will differentiate Kibana from REST, this String value will likely come from the request
            metrics.failed(clientId);
            planTelemetryManager.publish(planTelemetry, false);
            listener.onFailure(ex);
        });
        // Wrap it in a listener so that if we have any exceptions during execution, the listener picks it up
        // and all the metrics are properly updated
        ActionListener.run(executeListener, l -> session.execute(request, executionInfo, planRunner, l));
    }

    public IndexResolver indexResolver() {
        return indexResolver;
    }

    public Metrics metrics() {
        return this.metrics;
    }
}
