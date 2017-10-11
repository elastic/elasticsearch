/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.session.SqlSettings;

import java.util.function.Function;
import java.util.function.Supplier;

public class PlanExecutor {
    private final Client client;
    private final Supplier<ClusterState> stateSupplier;
    private final Function<ClusterState, Catalog> catalogSupplier;

    private final SqlParser parser;
    private final FunctionRegistry functionRegistry;
    private final Optimizer optimizer;
    private final Planner planner;

    public PlanExecutor(Client client, Supplier<ClusterState> stateSupplier,
            Function<ClusterState, Catalog> catalogSupplier) {
        this.client = client;
        this.stateSupplier = stateSupplier;
        this.catalogSupplier = catalogSupplier;

        this.parser = new SqlParser();
        this.functionRegistry = new DefaultFunctionRegistry();

        this.optimizer = new Optimizer();
        this.planner = new Planner();
    }

    public SqlSession newSession(SqlSettings settings) {
        Catalog catalog = catalogSupplier.apply(stateSupplier.get());
        return new SqlSession(settings, client, catalog, functionRegistry, parser, optimizer, planner);
    }


    public SearchSourceBuilder searchSource(String sql, SqlSettings settings) {
        PhysicalPlan executable = newSession(settings).executable(sql);
        if (executable instanceof EsQueryExec) {
            EsQueryExec e = (EsQueryExec) executable;
            return SourceGenerator.sourceBuilder(e.queryContainer(), settings.pageSize());
        }
        else {
            throw new PlanningException("Cannot generate a query DSL for %s", sql);
        }
    }

    public void sql(String sql, ActionListener<RowSet> listener) {
        sql(SqlSettings.EMPTY, sql, listener);
    }

    public void sql(SqlSettings sqlSettings, String sql, ActionListener<RowSet> listener) {
        SqlSession session = newSession(sqlSettings);
        try {
            PhysicalPlan executable = session.executable(sql);
            executable.execute(session, listener);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public void nextPage(Cursor cursor, ActionListener<RowSet> listener) {
        cursor.nextPage(client, listener);
    }
}
