/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;

public interface OperatorOnlyRegistry {

    /**
     * Check whether the given action and request qualify as operator-only. The method returns
     * null if the action+request is NOT operator-only. Other it returns a violation object
     * that contains the message for details.
     */
    OperatorPrivilegesViolation check(String action, TransportRequest request);

    /**
     * Checks to see if a given {@link RestHandler} is subject to operator-only restrictions for the REST API. Any REST API may be
     * fully or partially restricted. A fully restricted REST API mandates that the implementation call restChannel.sendResponse(...) and
     * return a {@link OperatorPrivilegesViolation}. A partially restricted REST API mandates that the {@link RestRequest} is marked as
     * restricted so that the downstream handler can behave appropriately. For example, to restrict the REST response the implementation
     * should call {@link RestRequest#markResponseRestricted(String)} so that the downstream handler can properly restrict the response
     * before returning to the client. Note - a partial restriction should return null.
     * @param restHandler The {@link RestHandler} to check for any restrictions
     * @param restRequest The {@link RestRequest} to check for any restrictions and mark any partially restricted REST API's
     * @param restChannel The {@link RestChannel} to enforce fully restricted REST API's
     * @return {@link RestRequest} to use when calling the {@link RestHandler}, can be same or different object as the passed in parameter
     */
    OperatorPrivilegesViolation checkRest(RestHandler restHandler, RestRequest restRequest, RestChannel restChannel);

}
