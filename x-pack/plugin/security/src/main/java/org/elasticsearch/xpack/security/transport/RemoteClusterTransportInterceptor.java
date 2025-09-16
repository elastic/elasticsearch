/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.util.Map;

/**
 * Allows to provide remote cluster interception that's capable of intercepting remote connections
 * both on the receiver and the sender side.
 */
public interface RemoteClusterTransportInterceptor {

    /**
     * Allows to intercept all transport requests on the sender side.
     */
    TransportInterceptor.AsyncSender interceptSender(TransportInterceptor.AsyncSender sender);

    /**
     * This method returns {@code true} if the outbound {@code connection} is targeting a remote cluster.
     */
    boolean isRemoteClusterConnection(Transport.Connection connection);

    /**
     * Allows interceptors to provide a custom {@link ServerTransportFilter} implementations per transport profile.
     * The transport filter is called on the receiver side to filter incoming requests
     * and execute authentication and authorization for all requests.
     *
     * @return map of {@link ServerTransportFilter}s per transport profile name
     */
    Map<String, ServerTransportFilter> getProfileTransportFilters(
        Map<String, SslProfile> profileConfigurations,
        DestructiveOperations destructiveOperations
    );

    /**
     * Returns {@code true} if any of the remote cluster access headers are in the security context.
     * This method is used to assert we don't have access headers already in the security context,
     * before we even run remote cluster intercepts. Serves as an integrity check that we properly clear
     * the security context between requests.
     */
    boolean hasRemoteClusterAccessHeadersInContext(SecurityContext securityContext);

}
