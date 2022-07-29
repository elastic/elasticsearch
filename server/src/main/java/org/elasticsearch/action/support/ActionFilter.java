/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.tasks.Task;

/**
 * A filter allowing to filter transport actions
 */
public interface ActionFilter {

    /**
     * The position of the filter in the chain. Execution is done from lowest order to highest.
     */
    int order();

    /**
     * Enables filtering the execution of an action on the request side, either by sending a response through the
     * {@link ActionListener} or by continuing the execution through the given {@link ActionFilterChain chain}
     */
    <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    );

    /**
     * A simple base class for injectable action filters that spares the implementation from handling the
     * filter chain. This base class should serve any action filter implementations that doesn't require
     * to apply async filtering logic.
     */
    abstract class Simple implements ActionFilter {
        @Override
        public final <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ActionFilterChain<Request, Response> chain
        ) {
            if (apply(action, request, listener)) {
                chain.proceed(task, action, request, listener);
            }
        }

        /**
         * Applies this filter and returns {@code true} if the execution chain should proceed, or {@code false}
         * if it should be aborted since the filter already handled the request and called the given listener.
         */
        protected abstract boolean apply(String action, ActionRequest request, ActionListener<?> listener);
    }
}
