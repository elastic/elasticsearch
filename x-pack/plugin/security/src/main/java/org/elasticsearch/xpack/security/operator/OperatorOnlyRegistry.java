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
     * Checks to see if a given {@link RestHandler} is subject to full restrictions for the REST API.
     *
     * A fully restricted REST API mandates that the implementation of this method throw an
     * {@link org.elasticsearch.ElasticsearchStatusException} with an appropriate status code and error message.
     *
     * An API may also be partially restricted: available to all users but with request or response restrictions,
     * (e.g., certain fields are not allowed in the request).
     * Partial restrictions are handled by the REST handler itself, not by this method.
     *
     * @param restHandler The {@link RestHandler} to check for any full restrictions
     * @param restRequest The {@link RestRequest} to provide context for restriction failures
     * @throws ElasticsearchStatusException if the request should be denied due to a full restriction
     */
    void checkRest(RestHandler restHandler, RestRequest restRequest) throws ElasticsearchException;

}
