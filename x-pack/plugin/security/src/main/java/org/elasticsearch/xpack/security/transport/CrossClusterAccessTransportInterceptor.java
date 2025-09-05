/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteConnectionManager;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class CrossClusterAccessTransportInterceptor implements RemoteClusterTransportInterceptor {

    private static final Logger logger = LogManager.getLogger(CrossClusterAccessTransportInterceptor.class);

    private final Function<
        Transport.Connection,
        Optional<RemoteConnectionManager.RemoteClusterAliasWithCredentials>> remoteClusterCredentialsResolver;
    private final CrossClusterAccessAuthenticationService crossClusterAccessAuthcService;
    private final AuthenticationService authcService;
    private final AuthorizationService authzService;
    private final XPackLicenseState licenseState;
    private final SecurityContext securityContext;
    private final ThreadPool threadPool;
    private final Settings settings;

    public CrossClusterAccessTransportInterceptor(
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        AuthenticationService authcService,
        AuthorizationService authzService,
        XPackLicenseState licenseState,
        ThreadPool threadPool,
        Settings settings,
        SecurityContext securityContext
    ) {
        this(
            crossClusterAccessAuthcService,
            authcService,
            authzService,
            licenseState,
            securityContext,
            threadPool,
            settings,
            RemoteConnectionManager::resolveRemoteClusterAliasWithCredentials
        );
    }

    // package-protected for testing
    CrossClusterAccessTransportInterceptor(
        CrossClusterAccessAuthenticationService crossClusterAccessAuthcService,
        AuthenticationService authcService,
        AuthorizationService authzService,
        XPackLicenseState licenseState,
        SecurityContext securityContext,
        ThreadPool threadPool,
        Settings settings,
        Function<Transport.Connection, Optional<RemoteConnectionManager.RemoteClusterAliasWithCredentials>> remoteClusterCredentialsResolver
    ) {
        this.remoteClusterCredentialsResolver = remoteClusterCredentialsResolver;
        this.crossClusterAccessAuthcService = crossClusterAccessAuthcService;
        this.authcService = authcService;
        this.authzService = authzService;
        this.licenseState = licenseState;
        this.securityContext = securityContext;
        this.threadPool = threadPool;
        this.settings = settings;
    }

    @Override
    public TransportInterceptor.AsyncSender interceptSender(TransportInterceptor.AsyncSender sender) {
        return null;
    }

    @Override
    public boolean isRemoteClusterConnection(Transport.Connection connection) {
        return false;
    }

    @Override
    public Map<String, ServerTransportFilter> getProfileFilters(
        Map<String, SslProfile> profileConfigurations,
        DestructiveOperations destructiveOperations
    ) {
        return Map.of();
    }

}
