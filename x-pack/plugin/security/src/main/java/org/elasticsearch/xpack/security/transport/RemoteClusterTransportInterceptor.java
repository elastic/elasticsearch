/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.util.Optional;

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
     * Allows interceptors to provide a custom {@link ServerTransportFilter} implementation
     * for intercepting requests for {@link RemoteClusterPortSettings#REMOTE_CLUSTER_PROFILE}
     * transport profile.
     * <p>
     * The transport filter is called on the receiver side to filter incoming remote cluster requests
     * and to execute authentication and authorization for all incoming requests.
     * <p>
     * This method is only called when setting {@link RemoteClusterPortSettings#REMOTE_CLUSTER_SERVER_ENABLED}
     * is set to {@code true}.
     *
     * @return a custom {@link ServerTransportFilter}s for the given transport profile,
     *         or an empty optional to fall back to the default transport filter
     */
    Optional<ServerTransportFilter> getRemoteProfileTransportFilter(SslProfile sslProfile, DestructiveOperations destructiveOperations);

    /**
     * Returns {@code true} if any of the remote cluster access headers are in the security context.
     * This method is used to assert we don't have access headers already in the security context,
     * before we even run remote cluster intercepts. Serves as an integrity check that we properly clear
     * the security context between requests.
     */
    boolean hasRemoteClusterAccessHeadersInContext(SecurityContext securityContext);

}
