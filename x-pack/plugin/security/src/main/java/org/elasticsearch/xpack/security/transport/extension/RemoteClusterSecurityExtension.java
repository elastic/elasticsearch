/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.extension;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.RemoteClusterAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.transport.CrossClusterAccessSecurityExtension;
import org.elasticsearch.xpack.security.transport.RemoteClusterTransportInterceptor;

import java.util.List;

/**
 * Allows defining an SPI extension point for providing a custom remote cluster security interceptor
 * and authentication service.
 *
 * <p>
 *  Currently, the SPI extension point only allows providing a single remote cluster security extension.
 *  If none is provided, it will fall back to {@link CrossClusterAccessSecurityExtension} by default.
 */
public interface RemoteClusterSecurityExtension {

    /**
     * Returns a {@link RemoteClusterTransportInterceptor} that allows intercepting remote connections
     * both on the receiver and the sender side.
     */
    RemoteClusterTransportInterceptor getTransportInterceptor();

    /**
     * Returns a {@link RemoteClusterAuthenticationService} used to authenticate remote cluster requests.
     */
    RemoteClusterAuthenticationService getAuthenticationService();

    /**
     * An SPI interface for providing remote cluster security extensions.
     * Allows to override the default extension provider ({@link CrossClusterAccessSecurityExtension.Provider}).
     */
    interface Provider {

        /**
         * Provides a custom {@link RemoteClusterSecurityExtension}.
         *
         * @param components security components that may be used to build remove cluster security extension services
         * @return a non-null remote cluster extension
         */
        RemoteClusterSecurityExtension getExtension(Components components);

        /**
         * Allows remote cluster extensions to provide settings.
         */
        default List<Setting<?>> getSettings() {
            return List.of();
        }

    }

    /**
     * Provides access to components that can be used by interceptor and authentication service.
     */
    interface Components {

        AuthenticationService authenticationService();

        AuthorizationService authorizationService();

        SecurityContext securityContext();

        ApiKeyService apiKeyService();

        ResourceWatcherService resourceWatcherService();

        ProjectResolver projectResolver();

        XPackLicenseState licenseState();

        ClusterService clusterService();

        Environment environment();

        ThreadPool threadPool();

        Settings settings();

        Client client();

    }

}
