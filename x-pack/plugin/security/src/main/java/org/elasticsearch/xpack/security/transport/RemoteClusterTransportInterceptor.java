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
     * Returns {@code true} if the {@code connection} is targeting a remote cluster.
     */
    boolean isRemoteClusterConnection(Transport.Connection connection);

    /**
     * Allows providing custom {@link ServerTransportFilter} implementations per transport "profile".
     * The transport filter is called on the receiver side to filter incoming requests.
     */
    Map<String, ServerTransportFilter> getProfileFilters(
        Map<String, SslProfile> profileConfigurations,
        DestructiveOperations destructiveOperations
    );

}
