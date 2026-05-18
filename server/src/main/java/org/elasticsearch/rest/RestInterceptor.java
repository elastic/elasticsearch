/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.action.ActionListener;

/**
 * Wraps the execution of a {@link RestHandler}
 */
@FunctionalInterface
public interface RestInterceptor {

    /**
     * @param listener The interceptor responds with {@code True} if the handler should be called,
     *                 or {@code False} if the request has been entirely handled by the interceptor.
     *                 In the case of {@link ActionListener#onFailure(Exception)}, the target handler
     *                 will not be called, the request will be treated as unhandled, and the regular
     *                 rest exception handling will be performed
     */
    void intercept(RestRequest request, RestChannel channel, RestHandler targetHandler, ActionListener<Boolean> listener) throws Exception;
}
