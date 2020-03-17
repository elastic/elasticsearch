/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import static org.elasticsearch.action.ActionListener.wrap;

public class EqlSession {

    private final Client client;
    private final Configuration configuration;
    private final IndexResolver indexResolver;

    private final PreAnalyzer preAnalyzer;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private final Planner planner;

    public EqlSession(Client client, Configuration cfg, IndexResolver indexResolver, PreAnalyzer preAnalyzer, Analyzer analyzer,
            Optimizer optimizer, Planner planner, PlanExecutor planExecutor) {
        
        this.client = client;
        this.configuration = cfg;
        this.indexResolver = indexResolver;
        this.preAnalyzer = preAnalyzer;
        this.analyzer = analyzer;
        this.optimizer = optimizer;
        this.planner = planner;
    }

    public Client client() {
        return client;
    }

    public Optimizer optimizer() {
        return optimizer;
    }

    public Configuration configuration() {
        return configuration;
    }

    public void eql(String eql, ParserParams params, ActionListener<Results> listener) {
        eqlExecutable(eql, params, wrap(e -> e.execute(this, listener), listener::onFailure));
    }
    
    public void eqlExecutable(String eql, ParserParams params, ActionListener<PhysicalPlan> listener) {
        try {
            physicalPlan(doParse(eql, params), listener);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public void physicalPlan(LogicalPlan optimized, ActionListener<PhysicalPlan> listener) {
        optimizedPlan(optimized, wrap(o -> listener.onResponse(planner.plan(o)), listener::onFailure));
    }

    public void optimizedPlan(LogicalPlan verified, ActionListener<LogicalPlan> listener) {
        analyzedPlan(verified, wrap(v -> listener.onResponse(optimizer.optimize(v)), listener::onFailure));
    }

    public void analyzedPlan(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, wrap(p -> listener.onResponse(analyzer.analyze(p)), listener::onFailure));
    }

    private <T> void preAnalyze(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        String indexWildcard = Strings.arrayToCommaDelimitedString(configuration.indices());

        indexResolver.resolveAsMergedMapping(indexWildcard, null, configuration.includeFrozen(), wrap(r -> {
            listener.onResponse(preAnalyzer.preAnalyze(parsed, r));
        }, listener::onFailure));
    }

    private LogicalPlan doParse(String eql, ParserParams params) {
        return new EqlParser().createStatement(eql, params);
    }
}