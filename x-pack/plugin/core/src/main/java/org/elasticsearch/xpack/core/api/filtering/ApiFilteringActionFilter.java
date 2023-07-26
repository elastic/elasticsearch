/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.api.filtering;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

public abstract class ApiFilteringActionFilter<Res extends ActionResponse> implements ActionFilter {

    private final ThreadContext threadContext;

    public ApiFilteringActionFilter(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        final ActionListener<Response> responseFilteringListener;
        if (isOperator() == false && getActionName().equals(action)) {
            responseFilteringListener = listener.map(this::filter);
        } else {
            responseFilteringListener = listener;
        }
        chain.proceed(task, action, request, responseFilteringListener);
    }

    @SuppressWarnings("unchecked")
    private <Response extends ActionResponse> Response filter(Response response) throws Exception {
        if (response.getClass().equals(getResponseClass())) {
            return (Response) getFilteringFunction().apply((Res) response);
        } else {
            return response;
        }
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(
            threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY)
        );
    }

    public abstract String getActionName();

    public abstract Class<Res> getResponseClass();

    public abstract CheckedFunction<Res, Res, Exception> getFilteringFunction();
}
