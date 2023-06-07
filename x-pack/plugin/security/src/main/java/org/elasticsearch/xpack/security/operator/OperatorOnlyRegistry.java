/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.transport.TransportRequest;

public interface OperatorOnlyRegistry {

    /**
     * Check whether the given action and request qualify as operator-only. The method returns
     * null if the action+request is NOT operator-only. Other it returns a violation object
     * that contains the message for details.
     */
    OperatorPrivilegesViolation check(String action, TransportRequest request);

    /**
     * Checks to see if a given {@link RestHandler} is subject to restrictions. This method may return a {@link RestResponse} if the
     * request is restricted due to operator privileges. Null will be returned if there are no restrictions. Callers must short-circuit
     * the request and respond back to the client with the given {@link RestResponse} without ever calling the {@link RestHandler}.
     * If null is returned the caller must proceed as normal and call the {@link RestHandler}.
     * @param restHandler The {@link RestHandler} to check for any restrictions
     * @param restRequest The {@link RestRequest} to check for any restrictions
     * @return null if no restrictions should be enforced, {@link RestResponse} if the request is restricted
     */
    RestResponse checkRestFull(RestHandler restHandler, RestRequest restRequest);

    /**
     * Checks to see if a given {@link RestHandler} is subject to partial restrictions. A partial restriction still allows the request
     * to proceed but will update/rewrite the provived {@link RestRequest} such that when the {@link RestHandler} is called the result
     * is variant (likley a restricted view) of the response provided. The caller must use the returned {@link RestRequest} when calling
     * the {@link RestHandler}
     * @param restHandler The {@link RestHandler} to check for any restrictions
     * @param restRequest The {@link RestRequest} to check for any restrictions
     * @return {@link RestRequest} to use when calling the {@link RestHandler}, can be same or different object as the passed in parameter
     */
    RestRequest checkRestPartial(RestHandler restHandler, RestRequest restRequest);

}
