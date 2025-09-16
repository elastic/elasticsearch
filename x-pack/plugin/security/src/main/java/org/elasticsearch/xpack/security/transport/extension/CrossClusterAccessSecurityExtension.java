/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.extension;

import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.transport.CrossClusterAccessTransportInterceptor;

import java.util.concurrent.atomic.AtomicReference;

public class CrossClusterAccessSecurityExtension implements RemoteClusterSecurityExtension {

    private final CrossClusterAccessAuthenticationService authenticationService;
    private final CrossClusterAccessTransportInterceptor transportInterceptor;

    public CrossClusterAccessSecurityExtension(RemoteClusterSecurityExtension.Components components) {
        this.authenticationService = new CrossClusterAccessAuthenticationService(
            components.clusterService(),
            components.apiKeyService(),
            components.authenticationService()
        );
        this.transportInterceptor = new CrossClusterAccessTransportInterceptor(
            components.settings(),
            components.threadPool(),
            components.authenticationService(),
            components.authorizationService(),
            components.securityContext(),
            this.authenticationService,
            components.licenseState()
        );
    }

    @Override
    public CrossClusterAccessTransportInterceptor getTransportInterceptor() {
        return this.transportInterceptor;
    }

    @Override
    public CrossClusterAccessAuthenticationService getAuthenticationService() {
        return this.authenticationService;
    }

    public static class Provider implements RemoteClusterSecurityExtension.Provider {

        private static final AtomicReference<CrossClusterAccessSecurityExtension> instance = new AtomicReference<>();

        @Override
        public RemoteClusterSecurityExtension getExtension(Components components) {
            return instance.updateAndGet(current -> current != null ? current : new CrossClusterAccessSecurityExtension(components));
        }
    }
}
