/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.eql.analysis.PostAnalyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.ql.util.ActionListeners.map;

public class EqlSession {

    private final Client client;
    private final EqlConfiguration configuration;
    private final IndexResolver indexResolver;

    private final PreAnalyzer preAnalyzer;
    private final PostAnalyzer postAnalyzer;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private final Planner planner;
    private final CircuitBreaker circuitBreaker;

    public EqlSession(
        Client client,
        EqlConfiguration cfg,
        IndexResolver indexResolver,
        PreAnalyzer preAnalyzer,
        PostAnalyzer postAnalyzer,
        FunctionRegistry functionRegistry,
        Verifier verifier,
        Optimizer optimizer,
        Planner planner,
        CircuitBreaker circuitBreaker
    ) {

        this.client = new ParentTaskAssigningClient(client, cfg.getTaskId());
        this.configuration = cfg;
        this.indexResolver = indexResolver;
        this.preAnalyzer = preAnalyzer;
        this.postAnalyzer = postAnalyzer;
        this.analyzer = new Analyzer(new AnalyzerContext(cfg, functionRegistry), verifier);
        this.optimizer = optimizer;
        this.planner = planner;
        this.circuitBreaker = circuitBreaker;
    }

    public Client client() {
        return client;
    }

    public Optimizer optimizer() {
        return optimizer;
    }

    public EqlConfiguration configuration() {
        return configuration;
    }

    public CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }

    public void eql(String eql, ParserParams params, ActionListener<Results> listener) {
        eqlExecutable(eql, params, wrap(e -> e.execute(this, map(listener, Results::fromPayload)), listener::onFailure));
    }

    public void eqlExecutable(String eql, ParserParams params, ActionListener<PhysicalPlan> listener) {
        try {
            physicalPlan(doParse(eql, params), listener);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public void physicalPlan(LogicalPlan optimized, ActionListener<PhysicalPlan> listener) {
        optimizedPlan(optimized, map(listener, planner::plan));
    }

    public void optimizedPlan(LogicalPlan verified, ActionListener<LogicalPlan> listener) {
        analyzedPlan(verified, map(listener, optimizer::optimize));
    }

    public void analyzedPlan(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, map(listener, p -> postAnalyze(analyzer.analyze(p))));
    }

    private <T> void preAnalyze(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        String indexWildcard = configuration.indexAsWildcard();
        if (configuration.isCancelled()) {
            listener.onFailure(new TaskCancelledException("cancelled"));
            return;
        }
        indexResolver.resolveAsMergedMapping(
            indexWildcard,
            configuration.indicesOptions(),
            configuration.runtimeMappings(),
            map(listener, r -> preAnalyzer.preAnalyze(parsed, r))
        );
    }

    private LogicalPlan postAnalyze(LogicalPlan verified) {
        return postAnalyzer.postAnalyze(verified, configuration);
    }

    private static LogicalPlan doParse(String eql, ParserParams params) {
        return new EqlParser().createStatement(eql, params);
    }
}
