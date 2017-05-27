/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.function.Function;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;

public class SqlSession {

    private final Client client;

    private final SqlParser parser;
    private final Catalog catalog;
    private final FunctionRegistry functionRegistry;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private final Planner planner;

    private final SqlSettings defaults;
    private SqlSettings settings;

    public SqlSession(SqlSession other) {
        this(other.defaults(), other.client(), other.parser, other.catalog(), other.functionRegistry(), other.analyzer(), other.optimizer(), other.planner());
    }

    public SqlSession(SqlSettings defaults, 
            Client client, SqlParser parser, Catalog catalog,
            FunctionRegistry functionRegistry, Analyzer analyzer, Optimizer optimizer, Planner planner) {
        this.client = client;

        this.parser = parser;
        this.catalog = catalog;
        this.functionRegistry = functionRegistry;
        this.analyzer = analyzer;
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
        return verify ? analyzer.verify(analyzer.analyze(plan)) : analyzer.analyze(plan);
    }

    public LogicalPlan optimizedPlan(LogicalPlan verified) {
        return optimizer.optimize(analyzedPlan(verified, true));
    }

    public PhysicalPlan physicalPlan(LogicalPlan optimized, boolean verify) {
        return planner.plan(optimizedPlan(optimized), verify);
    }

    public PhysicalPlan executable(String sql) {
        return physicalPlan(parse(sql), true);
    }

    public SqlSettings defaults() {
        return defaults;
    }

    public SqlSettings settings() {
        return settings;
    }

    public SqlSettings updateSettings(Function<Settings, Settings> transformer) {
        settings = new SqlSettings(transformer.apply(settings.cfg()));
        return settings;
    }

    public void execute(PhysicalPlan plan, ActionListener<RowSetCursor> listener) {
        plan.execute(this, listener);
    }
}