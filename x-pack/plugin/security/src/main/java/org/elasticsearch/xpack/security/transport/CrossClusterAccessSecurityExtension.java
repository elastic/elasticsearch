/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.support.ReloadableSecurityComponent;
import org.elasticsearch.xpack.security.transport.extension.RemoteClusterSecurityExtension;

import java.util.List;

/**
 * Remote cluster security extension point which is based on cross-cluster API keys.
 */
public class CrossClusterAccessSecurityExtension implements RemoteClusterSecurityExtension, ReloadableSecurityComponent {

    private static final Logger logger = LogManager.getLogger(CrossClusterAccessSecurityExtension.class);

    private final CrossClusterAccessAuthenticationService authenticationService;
    private final CrossClusterAccessTransportInterceptor transportInterceptor;

    private final CrossClusterApiKeySigningConfigReloader crossClusterApiKeySignerReloader;

    private CrossClusterAccessSecurityExtension(Components components) {
        this.crossClusterApiKeySignerReloader = new CrossClusterApiKeySigningConfigReloader(
            components.environment(),
            components.resourceWatcherService(),
            components.clusterService().getClusterSettings()
        );
        final CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager = new CrossClusterApiKeySignatureManager(
            components.environment()
        );
        crossClusterApiKeySignerReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);

        this.authenticationService = new CrossClusterAccessAuthenticationService(
            components.clusterService(),
            components.apiKeyService(),
            components.authenticationService(),
            crossClusterApiKeySignatureManager.verifier()
        );
        this.transportInterceptor = new CrossClusterAccessTransportInterceptor(
            components.settings(),
            components.threadPool(),
            components.authenticationService(),
            components.authorizationService(),
            components.securityContext(),
            this.authenticationService,
            crossClusterApiKeySignatureManager,
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

    @Override
    public void reload(final Settings settings) {
        crossClusterApiKeySignerReloader.reload(settings);
    }

    public static class Provider implements RemoteClusterSecurityExtension.Provider {

        @Override
        public RemoteClusterSecurityExtension getExtension(Components components) {
            logger.trace("Creating remote cluster security extension for [{}]", CrossClusterAccessSecurityExtension.class);
            return new CrossClusterAccessSecurityExtension(components);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return CrossClusterApiKeySigningSettings.getSettings();
        }

    }

}
