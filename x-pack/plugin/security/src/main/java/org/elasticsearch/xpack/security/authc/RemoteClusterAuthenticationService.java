/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.Map;

/**
 * Service interface for authenticating remote cluster requests.
 *
 * <p>
 * This service handles authentication for cross-cluster requests.
 * It provides methods to authenticate both full transport requests
 * and credential headers only.
 */
public interface RemoteClusterAuthenticationService {

    /**
     * Called to authenticates a remote cluster transport request.
     *
     * @param action the transport action being performed
     * @param request the transport request containing authentication headers
     * @param listener callback to receive the authenticated {@link Authentication}
     *                 object on success, or an exception on failure
     */
    void authenticate(String action, TransportRequest request, ActionListener<Authentication> listener);

    /**
     * Called early (after transport headers were received) to authenticate a remote cluster transport request.
     *
     * @param headers map of request headers containing authentication credentials
     * @param listener callback to receive {@code null} on successful authentication,
     *                 or an exception on authentication failure
     */
    void authenticateHeaders(Map<String, String> headers, ActionListener<Void> listener);

}
