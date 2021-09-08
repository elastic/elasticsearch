/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.eql.stats.QueryMetric;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {
    private final Client client;

    private final IndexResolver indexResolver;
    private final FunctionRegistry functionRegistry;

    private final PreAnalyzer preAnalyzer;
    private final PostAnalyzer postAnalyzer;
    private final Verifier verifier;
    private final Optimizer optimizer;
    private final Planner planner;
    private final CircuitBreaker circuitBreaker;

    private final Metrics metrics;


    public PlanExecutor(Client client, IndexResolver indexResolver, CircuitBreaker circuitBreaker) {
        this.client = client;
        this.indexResolver = indexResolver;
        this.circuitBreaker = circuitBreaker;

        this.functionRegistry = new EqlFunctionRegistry();

        this.metrics = new Metrics();

        this.preAnalyzer = new PreAnalyzer();
        this.postAnalyzer = new PostAnalyzer();
        this.verifier = new Verifier(metrics);
        this.optimizer = new Optimizer();
        this.planner = new Planner();
    }

    private EqlSession newSession(EqlConfiguration cfg) {
        return new EqlSession(
            client,
            cfg,
            indexResolver,
            preAnalyzer,
            postAnalyzer,
            functionRegistry,
            verifier,
            optimizer,
            planner,
            circuitBreaker
        );
    }

    public void eql(EqlConfiguration cfg, String eql, ParserParams parserParams, ActionListener<Results> listener) {
        metrics.total(QueryMetric.ALL);
        newSession(cfg).eql(eql, parserParams, wrap(listener::onResponse, ex -> {
            metrics.failed(QueryMetric.ALL);
            listener.onFailure(ex);
        }));
    }

    public Metrics metrics() {
        return this.metrics;
    }
}
