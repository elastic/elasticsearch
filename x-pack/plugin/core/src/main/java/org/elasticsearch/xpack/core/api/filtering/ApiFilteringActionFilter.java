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
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;

import static org.elasticsearch.xpack.core.security.operator.OperatorPrivilegesUtil.isOperator;

public abstract class ApiFilteringActionFilter<Res extends ActionResponse> implements MappedActionFilter {

    private final ThreadContext threadContext;
    private final String actionName;
    private final Class<Res> responseClass;

    protected ApiFilteringActionFilter(ThreadContext threadContext, String actionName, Class<Res> responseClass) {
        assert threadContext != null : "threadContext cannot be null";
        assert actionName != null : "actionName cannot be null";
        assert responseClass != null : "responseClass cannot be null";
        this.threadContext = threadContext;
        this.actionName = actionName;
        this.responseClass = responseClass;
    }

    @Override
    public final String actionName() {
        return actionName;
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
        if (isOperator(threadContext) == false && actionName.equals(action)) {
            responseFilteringListener = listener.map(this::filter);
        } else {
            responseFilteringListener = listener;
        }
        chain.proceed(task, action, request, responseFilteringListener);
    }

    @SuppressWarnings("unchecked")
    private <Response extends ActionResponse> Response filter(Response response) throws Exception {
        if (response.getClass().equals(responseClass)) {
            return (Response) filterResponse((Res) response);
        } else {
            return response;
        }
    }

    protected abstract Res filterResponse(Res response) throws Exception;
}
