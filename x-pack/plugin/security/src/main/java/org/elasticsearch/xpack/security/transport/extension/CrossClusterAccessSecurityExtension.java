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

/**
 * Remote cluster security extension point which is based on cross-cluster API keys.
 */
public class CrossClusterAccessSecurityExtension implements RemoteClusterSecurityExtension {

    private static final AtomicReference<InstancesHolder> instancesHolder = new AtomicReference<>();

    @Override
    public CrossClusterAccessTransportInterceptor getTransportInterceptor(Components components) {
        return instancesHolder(components).transportInterceptor;
    }

    @Override
    public CrossClusterAccessAuthenticationService getAuthenticationService(Components components) {
        return instancesHolder(components).authenticationService;
    }

    private InstancesHolder instancesHolder(Components components) {
        return instancesHolder.updateAndGet(current -> current != null ? current : new InstancesHolder(components));
    }

    private static class InstancesHolder {

        private final CrossClusterAccessAuthenticationService authenticationService;
        private final CrossClusterAccessTransportInterceptor transportInterceptor;

        private InstancesHolder(Components components) {
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
    }
}
