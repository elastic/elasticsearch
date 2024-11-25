/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Map;

/**
 * This provides helper methods for dealing with the execution of requests made using a {@link Client} such that they
 * have the origin as a transient, and listeners have the appropriate context upon invocation.
 */
public interface ClientAuthHelperService {
    /**
     * This method retrieves the security headers from the given threadContext.
     * @param threadContext The ThreadContext from which the security headers are retrieved.
     * @param clusterState Used to make sure the headers are written in a way that can be handled by all nodes in the cluster.
     */
    Map<String, String> getPersistableSafeSecurityHeaders(ThreadContext threadContext, ClusterState clusterState);

    /**
     * This method executes a client operation asynchronously. It uses auth from the given headers if present, falling back
     * to the given origin if there are no auth headers.
     *
     * @param headers
     *            Request headers, as retrieved by {@link #getPersistableSafeSecurityHeaders(ThreadContext, ClusterState)}
     * @param origin
     *            The origin to fall back to if there are no security headers
     * @param action
     *            The action to execute
     * @param request
     *            The request object for the action
     * @param listener
     *            The listener to call when the action is complete
     */
    <Request extends ActionRequest, Response extends ActionResponse> void executeWithHeadersAsync(
        Map<String, String> headers,
        String origin,
        Client client,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );
}
