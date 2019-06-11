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
 * A ConditionalClusterPrivilege is a composition of a {@link ClusterPrivilege} (that determines which actions may be executed) with a
 * {@link BiPredicate} for a {@link TransportRequest} (that determines which requests may be executed) and a {@link Authentication} (for
 * current authenticated user). The a given execution of an action is considered to be permitted if both the action and the request are
 * permitted in the context of given authentication.
 */
public interface ConditionalClusterPrivilege {

    /**
     * The action-level privilege that is required by this conditional privilege.
     */
    ClusterPrivilege getPrivilege();

    /**
     * The request-level privilege (as a {@link BiPredicate}) that is required by this conditional privilege.
     * Conditions can also be evaluated based on the {@link Authentication} details.
     */
    BiPredicate<TransportRequest, Authentication> getRequestPredicate();

}
