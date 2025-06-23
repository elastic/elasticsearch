/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

/**
 * Immutable implementation of {@link SecurityExtension.SecurityComponents}.
 */
public final class ExtensionComponents implements SecurityExtension.SecurityComponents {
    private final Environment environment;
    private final Client client;
    private final ClusterService clusterService;
    private final ResourceWatcherService resourceWatcherService;
    private final UserRoleMapper roleMapper;
    private final ProjectResolver projectResolver;

    public ExtensionComponents(
        Environment environment,
        Client client,
        ClusterService clusterService,
        ResourceWatcherService resourceWatcherService,
        UserRoleMapper roleMapper,
        ProjectResolver projectResolver
    ) {
        this.environment = environment;
        this.client = client;
        this.clusterService = clusterService;
        this.resourceWatcherService = resourceWatcherService;
        this.roleMapper = roleMapper;
        this.projectResolver = projectResolver;
    }

    @Override
    public Settings settings() {
        return environment.settings();
    }

    @Override
    public Environment environment() {
        return environment;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
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
    public UserRoleMapper roleMapper() {
        return roleMapper;
    }

    @Override
    public ProjectResolver projectResolver() {
        return projectResolver;
    }
}
