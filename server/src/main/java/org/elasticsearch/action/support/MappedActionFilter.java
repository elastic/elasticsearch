/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.tasks.Task;

/**
 * An action filter that is run only for a single action.
 *
 * Note: This is an independent interface from {@link ActionFilter} so that it does not
 * have an order. The relative order of executed MappedActionFilter with the same action name
 * is undefined.
 */
public interface MappedActionFilter {
    /** Return the name of the action for which this filter should be run */
    String actionName();

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
}
