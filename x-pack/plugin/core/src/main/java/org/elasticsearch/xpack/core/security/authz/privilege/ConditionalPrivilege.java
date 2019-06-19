/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.function.BiPredicate;

/**
 * A ConditionalPrivilege is an interface that helps adding a condition to a {@link Privilege}, that defines a {@link BiPredicate} for a
 * {@link TransportRequest} (that determines which requests may be executed) and a {@link Authentication} (for current authenticated user).
 * The predicate can be used to determine if both the action and the request are permitted in the context of given authentication.
 */
public interface ConditionalPrivilege {

    /**
     * The request-level privilege (as a {@link BiPredicate}) that is required by this conditional privilege.
     * Conditions can also be evaluated based on the {@link Authentication} details.
     */
    BiPredicate<TransportRequest, Authentication> getRequestPredicate();
}
