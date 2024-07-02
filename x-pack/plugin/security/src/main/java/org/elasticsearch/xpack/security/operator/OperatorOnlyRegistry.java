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
     * A partially restricted REST API is available to non-operator users but either the request or response for it is restricted
     * (e.g., certain fields are not allowed in the request).
     *
     * The implementation of this method should mark all {@link RestRequest}s for APIs that are *not* fully restricted as candidates
     * for partial restriction. It's the responsibility of the downstream REST handler to apply the necessary partial restrictions
     * (if any).
     * The implementation of this method should call {@link RestRequest#markApiRestrictionsActiveFor(String)} to indicate to downstream
     * handlers that any partial restrictions they have should be applied.
     *
     * @param restHandler The {@link RestHandler} to check for any restrictions
     * @param restRequest The {@link RestRequest} to check for any restrictions and mark all REST APIs that are not fully restricted
     * @throws ElasticsearchStatusException if the request should be denied in its entirety (fully restricted)
     */
    void checkRest(RestHandler restHandler, RestRequest restRequest) throws ElasticsearchException;

}
