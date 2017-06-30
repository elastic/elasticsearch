/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.expression.function.DefaultFunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.session.SqlSettings;

import java.io.IOException;
import java.util.TimeZone;
import java.util.function.Supplier;

public class PlanExecutor extends AbstractLifecycleComponent {
    // NOCOMMIT prefer not to use AbstractLifecycleComponent because the reasons for its tradeoffs is lost to the mists of time
    private static final SqlSettings DEFAULTS = SqlSettings.EMPTY;

    private final Client client;

    private final SqlParser parser;
    private Catalog catalog;
    private final FunctionRegistry functionRegistry;
    private final Analyzer analyzer;
    private final Optimizer optimizer;
    private final Planner planner;

    public PlanExecutor(Client client, Supplier<ClusterState> clusterState) {
        super(client.settings());

        this.client = client;
        this.catalog = new EsCatalog(clusterState);

        this.parser = new SqlParser();
        this.functionRegistry = new DefaultFunctionRegistry();
        this.analyzer = new Analyzer(catalog, functionRegistry);
        this.optimizer = new Optimizer(catalog);
        this.planner = new Planner();
    }

    public Catalog catalog() {
        return catalog;
    }

    public SqlSession newSession() {
        return new SqlSession(DEFAULTS, client, parser, catalog, functionRegistry, analyzer, optimizer, planner);
    }

    public void sql(String sql, TimeZone timeZone, ActionListener<RowSetCursor> listener) {
        SqlSession session = newSession();
        session.executable(sql, timeZone).execute(session, listener);
    }

    @Override
    protected void doStart() {
        //no-op
    }

    @Override
    protected void doStop() {
        //no-op
    }

    @Override
    protected void doClose() throws IOException {
        //no-op
    }
}
