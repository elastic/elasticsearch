/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
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
     * Checks to see if a given {@link RestHandler} is subject to operator-only restrictions for the REST API.
     *
     * Any REST API may be fully or partially restricted.
     * A fully restricted REST API mandates that the implementation of this method throw an
     * {@link org.elasticsearch.ElasticsearchStatusException} with an appropriate status code and error message.
     *
     * A partially restricted REST API mandates that the {@link RestRequest} is marked as restricted so that the downstream handler can
     * behave appropriately.
     * For example, to restrict the REST response the implementation
     * should call {@link RestRequest#markPathRestricted(String)} so that the downstream handler can properly restrict the response
     * before returning to the client. Note - a partial restriction should not throw an exception.
     *
     * @param restHandler The {@link RestHandler} to check for any restrictions
     * @param restRequest The {@link RestRequest} to check for any restrictions and mark any partially restricted REST API's
     * @throws ElasticsearchStatusException if the request should be denied in its entirety (fully restricted)
     */
    void checkRest(RestHandler restHandler, RestRequest restRequest) throws ElasticsearchException;

}
