/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.SqlQueryAction;
import org.elasticsearch.xpack.sql.plugin.SqlQueryRequest;
import org.elasticsearch.xpack.sql.plugin.SqlQueryResponse;
import org.elasticsearch.xpack.sql.plugin.TransportSqlQueryAction;

import java.util.Objects;

/**
 * Implements embedded sql mode by intercepting requests to SQL APIs and executing them locally.
 */
public class EmbeddedModeFilterClient extends FilterClient {
    private PlanExecutor planExecutor;

    public EmbeddedModeFilterClient(Client in) {
        super(in);
    }

    public void setPlanExecutor(PlanExecutor executor) {
        this.planExecutor = executor;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <         Request extends ActionRequest,
                        Response extends ActionResponse,
                        RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> 
                void doExecute(Action<Request, Response, RequestBuilder> action,
                        Request request, ActionListener<Response> listener) {
        Objects.requireNonNull(planExecutor, "plan executor not set on EmbeddedClient");
        
        if (action == SqlQueryAction.INSTANCE) {
            TransportSqlQueryAction.operation(planExecutor, (SqlQueryRequest) request, (ActionListener<SqlQueryResponse>) listener);
        } else {
            super.doExecute(action, request, listener);
        }
    }
}
