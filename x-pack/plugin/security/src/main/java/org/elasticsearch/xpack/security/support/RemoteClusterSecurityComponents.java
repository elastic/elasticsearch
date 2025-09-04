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
import org.elasticsearch.xpack.security.authz.AuthorizationService;

public class RemoteClusterSecurityComponents implements RemoteClusterSecurityExtension.Components {

    private final AuthenticationService authenticationService;
    private final AuthorizationService authorizationService;
    private final XPackLicenseState licenseState;
    private final ApiKeyService apiKeyService;
    private final ResourceWatcherService resourceWatcherService;
    private final ClusterService clusterService;
    private final Environment environment;
    private final ThreadPool threadPool;
    private final Settings settings;

    public RemoteClusterSecurityComponents(
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        XPackLicenseState licenseState,
        ApiKeyService apiKeyService,
        ResourceWatcherService resourceWatcherService,
        ClusterService clusterService,
        Environment environment,
        ThreadPool threadPool,
        Settings settings
    ) {
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.licenseState = licenseState;
        this.apiKeyService = apiKeyService;
        this.resourceWatcherService = resourceWatcherService;
        this.clusterService = clusterService;
        this.environment = environment;
        this.threadPool = threadPool;
        this.settings = settings;
    }

    @Override
    public AuthenticationService authenticationService() {
        return authenticationService;
    }

    @Override
    public AuthorizationService authorizationService() {
        return authorizationService;
    }

    @Override
    public XPackLicenseState licenseState() {
        return licenseState;
    }

    @Override
    public ApiKeyService apiKeyService() {
        return apiKeyService;
    }

    @Override
    public ResourceWatcherService resourceWatcherService() {
        return resourceWatcherService;
    }

    @Override
    public ClusterService clusterService() {
        return clusterService;
    }

    @Override
    public Environment environment() {
        return environment;
    }

    @Override
    public ThreadPool threadPool() {
        return threadPool;
    }

    @Override
    public Settings settings() {
        return settings;
    }
}
