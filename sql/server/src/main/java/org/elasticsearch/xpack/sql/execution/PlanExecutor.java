/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.plugin.SqlGetIndicesAction;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.session.SqlSettings;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PlanExecutor {
    private final Client client;
    private final Supplier<ClusterState> stateSupplier;
    /**
     * The way that we resolve indices asynchronously. This must
     * be passed in to support embedded mode. Otherwise we could
     * use the {@link #client} directly.
     */
    private final BiConsumer<SqlGetIndicesAction.Request, ActionListener<SqlGetIndicesAction.Response>> getIndices;
    private final Function<ClusterState, Catalog> catalogSupplier;

    private final SqlParser parser;
    private final FunctionRegistry functionRegistry;
    private final Optimizer optimizer;
    private final Planner planner;

    public PlanExecutor(Client client, Supplier<ClusterState> stateSupplier,
            BiConsumer<SqlGetIndicesAction.Request, ActionListener<SqlGetIndicesAction.Response>> getIndices,
            Function<ClusterState, Catalog> catalogSupplier) {
        this.client = client;
        this.stateSupplier = stateSupplier;
        this.getIndices = getIndices;
        this.catalogSupplier = catalogSupplier;

        this.parser = new SqlParser();
        this.functionRegistry = new DefaultFunctionRegistry();
        this.optimizer = new Optimizer();
        this.planner = new Planner();
    }

    public SqlSession newSession(SqlSettings settings) {
        return new SqlSession(settings, client, getIndices, catalogSupplier.apply(stateSupplier.get()), parser,
                functionRegistry, optimizer, planner);
    }

    public void sql(String sql, ActionListener<RowSetCursor> listener) {
        sql(SqlSettings.EMPTY, sql, listener);
    }

    public void sql(SqlSettings sqlSettings, String sql, ActionListener<RowSetCursor> listener) {
        SqlSession session = newSession(sqlSettings);
        session.executable(sql).execute(session, listener);
    }

    public void nextPage(Cursor cursor, ActionListener<RowSetCursor> listener) {
        cursor.nextPage(client, listener);
    }
}
