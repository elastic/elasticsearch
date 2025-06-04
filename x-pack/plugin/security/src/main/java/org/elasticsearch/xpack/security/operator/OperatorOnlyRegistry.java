/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.exception.ElasticsearchException;
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
     * This method is only called if the user is not an operator.
     * Implementations should fail the request if the {@link RestRequest} is not allowed to proceed by throwing an
     * {@link ElasticsearchException}. If the request should be handled by the associated {@link RestHandler},
     * then this implementations should do nothing.
     */
    void checkRest(RestHandler restHandler, RestRequest restRequest) throws ElasticsearchException;

}
