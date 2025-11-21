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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

public class RemoteClusterSecurityComponents implements RemoteClusterSecurityExtension.Components {

    private final AuthenticationService authenticationService;
    private final AuthorizationService authorizationService;
    private final SecurityContext securityContext;
    private final ApiKeyService apiKeyService;
    private final ResourceWatcherService resourceWatcherService;
    private final ProjectResolver projectResolver;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;
    private final Environment environment;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final Client client;

    public RemoteClusterSecurityComponents(
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        SecurityContext securityContext,
        ApiKeyService apiKeyService,
        ResourceWatcherService resourceWatcherService,
        ProjectResolver projectResolver,
        XPackLicenseState licenseState,
        ClusterService clusterService,
        Environment environment,
        ThreadPool threadPool,
        Settings settings,
        Client client
    ) {
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.securityContext = securityContext;
        this.apiKeyService = apiKeyService;
        this.resourceWatcherService = resourceWatcherService;
        this.projectResolver = projectResolver;
        this.licenseState = licenseState;
        this.clusterService = clusterService;
        this.environment = environment;
        this.threadPool = threadPool;
        this.settings = settings;
        this.client = client;
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
    public SecurityContext securityContext() {
        return securityContext;
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
    public ProjectResolver projectResolver() {
        return projectResolver;
    }

    @Override
    public XPackLicenseState licenseState() {
        return licenseState;
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

    @Override
    public Client client() {
        return client;
    }
}
