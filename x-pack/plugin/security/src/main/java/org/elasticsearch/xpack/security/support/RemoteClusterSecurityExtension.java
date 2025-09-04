/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.RemoteClusterAuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.transport.RemoteClusterTransportInterceptor;

public interface RemoteClusterSecurityExtension {

    RemoteClusterTransportInterceptor getTransportInterceptor();

    RemoteClusterAuthenticationService getRemoteClusterAuthenticationService();

    interface Provider {

        RemoteClusterSecurityExtension getExtension(Components components);

    }

    interface Components {

        AuthenticationService authenticationService();

        AuthorizationService authorizationService();

        XPackLicenseState licenseState();

        ApiKeyService apiKeyService();

        ResourceWatcherService resourceWatcherService();

        ClusterService clusterService();

        Environment environment();

        ThreadPool threadPool();

        Settings settings();

    }

}
