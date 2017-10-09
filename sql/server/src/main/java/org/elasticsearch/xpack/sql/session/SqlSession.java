/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.plugin.SqlGetIndicesAction;

import java.util.List;
import java.util.function.Function;

public class SqlSession {

    private final Client client;
    private final Catalog catalog;

    private final SqlParser parser;
    private final FunctionRegistry functionRegistry;
    private final Optimizer optimizer;
    private final Planner planner;
    private final Analyzer analyzer;

    private final SqlSettings defaults; // NOCOMMIT this doesn't look used - it is for RESET SESSION
    private SqlSettings settings;

    // thread-local used for sharing settings across the plan compilation
    // TODO investigate removing
    public static final ThreadLocal<SqlSettings> CURRENT_SETTINGS = new ThreadLocal<SqlSettings>() {
        @Override
        public String toString() {
            return "SQL Session";
        }
    };

    public SqlSession(SqlSession other) {
        this(other.defaults(), other.client(), other.catalog(), other.functionRegistry(),
                other.parser, other.optimizer(), other.planner());
    }

    public SqlSession(SqlSettings defaults, Client client,
            Catalog catalog, FunctionRegistry functionRegistry, 
            SqlParser parser, 
            Optimizer optimizer,
            Planner planner) {
        this.client = client;
        this.catalog = catalog;

        this.parser = parser;
        this.functionRegistry = functionRegistry;
        //TODO: analyzer should really be a singleton
        this.analyzer = new Analyzer(functionRegistry, catalog);
        this.optimizer = optimizer;
        this.planner = planner;

        this.defaults = defaults;
        this.settings = defaults;
    }

    public Catalog catalog() {
        return catalog;
    }

    public FunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public Client client() {
        return client;
    }

    /**
     * Get the indices matching a pattern. Prefer this method if possible.
     */
    public void getIndices(String[] patterns, IndicesOptions options, ActionListener<List<EsIndex>> listener) {
        SqlGetIndicesAction.Request request = new SqlGetIndicesAction.Request(options, patterns).local(true);
        client.execute(SqlGetIndicesAction.INSTANCE, request, ActionListener.wrap(response -> {
            listener.onResponse(response.indices());
        }, listener::onFailure));
    }

    public Planner planner() {
        return planner;
    }

    public Analyzer analyzer() {
        return analyzer;
    }

    public Optimizer optimizer() {
        return optimizer;
    }

    public LogicalPlan parse(String sql) {
        return parser.createStatement(sql);
    }

    public Expression expression(String expression) {
        return parser.createExpression(expression);
    }

    public LogicalPlan analyzedPlan(LogicalPlan plan, boolean verify) {
        Analyzer analyzer = analyzer();
        return verify ? analyzer.verify(analyzer.analyze(plan)) : analyzer.analyze(plan);
    }

    public LogicalPlan optimizedPlan(LogicalPlan verified) {
        return optimizer.optimize(analyzedPlan(verified, true));
    }

    public PhysicalPlan physicalPlan(LogicalPlan optimized, boolean verify) {
        return planner.plan(optimizedPlan(optimized), verify);
    }

    public PhysicalPlan executable(String sql) {
        CURRENT_SETTINGS.set(settings);
        try {
            return physicalPlan(parse(sql), true);
        } finally {
            CURRENT_SETTINGS.remove();
        }
    }

    public void sql(String sql, ActionListener<RowSet> listener) {
        executable(sql).execute(this, listener);
    }

    public SqlSettings defaults() {
        return defaults;
    }

    public SqlSettings settings() {
        return settings;
    }

    // session SET
    public SqlSettings updateSettings(Function<Settings, Settings> transformer) {
        settings = new SqlSettings(transformer.apply(settings.cfg()));
        return settings;
    }

    public void execute(PhysicalPlan plan, ActionListener<RowSet> listener) {
        plan.execute(this, listener);
    }
}