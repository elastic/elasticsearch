/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.TransportSqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.TransportSqlQueryAction;

import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Implements embedded sql mode by intercepting requests to SQL APIs and executing them locally.
 */
public class SqlNodeClient extends NodeClient {

    private final Client in;
    private final PlanExecutor planExecutor;

    SqlNodeClient(Client in) {
        super(in.settings(), in.threadPool());
        this.in = in;

        SearchModule searchModule = new SearchModule(Settings.EMPTY, true, singletonList(new MatrixAggregationPlugin()));
        NamedWriteableRegistry writeableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        IndexResolver indexResolver = new IndexResolver(in, "elasticsearch");
        this.planExecutor = new PlanExecutor(in, indexResolver, writeableRegistry);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task
    executeLocally(Action<Response> action, Request request, ActionListener<Response> listener) {
        doExecute(action, request, listener);
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void
    doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
        Objects.requireNonNull(planExecutor, "plan executor not set on EmbeddedClient");

        if (action == SqlQueryAction.INSTANCE) {
            TransportSqlQueryAction.operation(
                planExecutor, (SqlQueryRequest) request, (ActionListener<SqlQueryResponse>) listener, "marios", "debug-elasticsearch");
        } else if (action == SqlClearCursorAction.INSTANCE) {
            TransportSqlClearCursorAction.operation(planExecutor, (SqlClearCursorRequest) request,
                (ActionListener<SqlClearCursorResponse>) listener);
        } else {
            in.execute(action, request, listener);
        }
    }
}
