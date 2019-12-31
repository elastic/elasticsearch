/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

/**
 * Immutable implementation of {@link SecurityExtension.SecurityComponents}.
 */
public class ExtensionComponents implements SecurityExtension.SecurityComponents {
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final ResourceWatcherService resourceWatcherService;
    private final UserRoleMapper roleMapper;

    public ExtensionComponents(Client client, ThreadPool threadPool, ClusterService clusterService,
                               ResourceWatcherService resourceWatcherService, UserRoleMapper roleMapper) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.resourceWatcherService = resourceWatcherService;
        this.roleMapper = roleMapper;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public ThreadPool threadPool() {
        return threadPool;
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
}
