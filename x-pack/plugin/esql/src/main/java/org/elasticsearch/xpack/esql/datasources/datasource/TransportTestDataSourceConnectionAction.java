/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.TestConnectionResult;

/** Transport handler for {@link TestDataSourceConnectionAction}. Runs on a GENERIC thread to allow blocking I/O. */
public class TransportTestDataSourceConnectionAction extends HandledTransportAction<
    TestDataSourceConnectionAction.Request,
    TestDataSourceConnectionAction.Response> {

    private final DataSourceModule dataSourceModule;

    @Inject
    public TransportTestDataSourceConnectionAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        DataSourceModule dataSourceModule
    ) {
        super(TestDataSourceConnectionAction.NAME, transportService, actionFilters, in -> {
            throw new UnsupportedOperationException("action [" + TestDataSourceConnectionAction.NAME + "] is local-only");
        }, threadPool.executor(ThreadPool.Names.GENERIC));
        this.dataSourceModule = dataSourceModule;
    }

    @Override
    protected void doExecute(
        Task task,
        TestDataSourceConnectionAction.Request request,
        ActionListener<TestDataSourceConnectionAction.Response> listener
    ) {
        try {
            TestConnectionResult result = dataSourceModule.testConnection(request.type(), request.rawSettings());
            TestDataSourceConnectionAction.Response response = switch (result) {
                case TestConnectionResult.Success s -> TestDataSourceConnectionAction.Response.success();
                case TestConnectionResult.Failure f -> TestDataSourceConnectionAction.Response.failure(f.error());
                case TestConnectionResult.Untestable u -> TestDataSourceConnectionAction.Response.untestable();
            };
            listener.onResponse(response);
        } catch (IllegalArgumentException e) {
            // No factory is registered for this type — server configuration mismatch. Return 400 so the
            // caller knows the type itself is invalid, not the remote endpoint.
            listener.onFailure(new ElasticsearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST, e));
        }
    }
}
