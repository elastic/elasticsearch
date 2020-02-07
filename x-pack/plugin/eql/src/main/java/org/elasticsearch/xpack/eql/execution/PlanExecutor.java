/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.planner.Planner;
import org.elasticsearch.xpack.eql.session.Configuration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {
    private final Client client;
    private final NamedWriteableRegistry writableRegistry;

    private final IndexResolver indexResolver;
    private final FunctionRegistry functionRegistry;

    private final PreAnalyzer preAnalyzer;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private final Planner planner;

    public PlanExecutor(Client client, IndexResolver indexResolver, NamedWriteableRegistry writeableRegistry) {
        this.client = client;
        this.writableRegistry = writeableRegistry;

        this.indexResolver = indexResolver;
        this.functionRegistry = null;

        this.preAnalyzer = new PreAnalyzer();
        this.analyzer = new Analyzer(functionRegistry, new Verifier());
        this.optimizer = new Optimizer();
        this.planner = new Planner();
    }

    private EqlSession newSession(Configuration cfg) {
        return new EqlSession(client, cfg, indexResolver, preAnalyzer, analyzer, optimizer, planner, this);
    }

    public void eql(Configuration cfg, String eql, ParserParams parserParams, ActionListener<Results> listener) {
        newSession(cfg).eql(eql, parserParams, wrap(listener::onResponse, listener::onFailure));
    }
}
