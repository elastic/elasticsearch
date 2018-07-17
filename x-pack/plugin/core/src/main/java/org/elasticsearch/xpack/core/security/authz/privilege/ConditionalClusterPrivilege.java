/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.transport.TransportRequest;

import java.util.function.Predicate;

/**
 * A ConditionalClusterPrivilege is a composition of a {@link ClusterPrivilege} (that determines which actions may be executed)
 * with a {@link Predicate} for a {@link TransportRequest} (that determines which requests may be executed).
 * The a given execution of an action is considered to be permitted if both the action and the request are permitted.
 */
public interface ConditionalClusterPrivilege extends NamedWriteable {

    /**
     * The action-level privilege that is required by this conditional privilege.
     */
    ClusterPrivilege getPrivilege();

    /**
     * The request-level privilege (as a {@link Predicate}) that is required by this conditional privilege.
     */
    Predicate<TransportRequest> getRequestPredicate();

}
