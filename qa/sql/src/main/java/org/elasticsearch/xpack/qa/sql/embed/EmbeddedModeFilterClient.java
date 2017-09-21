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
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.plugin.sql.action.TransportSqlAction;

/**
 * Implements embedded sql mode by intercepting requests to SQL APIs and executing them locally.
 */
public class EmbeddedModeFilterClient extends FilterClient {
    private final PlanExecutor planExecutor;

    public EmbeddedModeFilterClient(Client in, PlanExecutor planExecutor) {
        super(in);
        this.planExecutor = planExecutor;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <         Request extends ActionRequest,
                        Response extends ActionResponse,
                        RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> 
                void doExecute(Action<Request, Response, RequestBuilder> action,
                        Request request, ActionListener<Response> listener) {
        if (action == SqlAction.INSTANCE) {
            TransportSqlAction.operation(planExecutor, (SqlRequest) request, (ActionListener<SqlResponse>) listener);
        } else {
            super.doExecute(action, request, listener);
        }
    }
}
