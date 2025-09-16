/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.extension;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.transport.CrossClusterAccessTransportInterceptor;

/**
 * Remote cluster security extension point which is based on cross-cluster API keys.
 */
public class CrossClusterAccessSecurityExtension implements RemoteClusterSecurityExtension {

    private final CrossClusterAccessAuthenticationService authenticationService;
    private final CrossClusterAccessTransportInterceptor transportInterceptor;

    private CrossClusterAccessSecurityExtension(Components components) {
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
        return transportInterceptor;
    }

    @Override
    public CrossClusterAccessAuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public static class Provider implements RemoteClusterSecurityExtension.Provider {

        private final SetOnce<CrossClusterAccessSecurityExtension> extension = new SetOnce<>();

        @Override
        public RemoteClusterSecurityExtension getExtension(Components components) {
            if (extension.get() == null) {
                extension.set(new CrossClusterAccessSecurityExtension(components));
            }
            return extension.get();
        }
    }

}
