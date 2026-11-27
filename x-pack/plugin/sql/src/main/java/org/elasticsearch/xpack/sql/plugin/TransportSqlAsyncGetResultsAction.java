/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ql.plugin.AbstractTransportQlAsyncGetResultsAction;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryTask;

public class TransportSqlAsyncGetResultsAction extends AbstractTransportQlAsyncGetResultsAction<SqlQueryResponse, SqlQueryTask> {

    /** Breaker label for a relayed get async SQL response. */
    public static final String RELAYED_RESPONSE_BREAKER_LABEL = "<async_sql_response>";

    @Inject
    public TransportSqlAsyncGetResultsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        Client client,
        ThreadPool threadPool,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService
    ) {
        super(
            SqlAsyncGetResultsAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            registry,
            client,
            threadPool,
            bigArrays,
            circuitBreakerService,
            SqlQueryTask.class
        );
    }

    @Override
    public Writeable.Reader<SqlQueryResponse> responseReader() {
        return SqlQueryResponse::new;
    }

    @Override
    protected String relayedResponseBreakerLabel() {
        return RELAYED_RESPONSE_BREAKER_LABEL;
    }
}
