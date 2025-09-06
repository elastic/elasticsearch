/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor.AsyncSender;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.util.Map;

public interface RemoteClusterTransportInterceptor {

    AsyncSender interceptSender(AsyncSender sender);

    boolean isRemoteClusterConnection(Transport.Connection connection);

    Map<String, ServerTransportFilter> getProfileFilters(
        Map<String, SslProfile> profileConfigurations,
        DestructiveOperations destructiveOperations
    );

}
