/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoriesModuleTests extends ESTestCase {

    private Environment environment;
    private NamedXContentRegistry contentRegistry;
    private ThreadPool threadPool;
    private List<RepositoryPlugin> repoPlugins = new ArrayList<>();
    private RepositoryPlugin plugin1;
    private RepositoryPlugin plugin2;
    private Repository.Factory factory;
    private TransportService transportService;
    private ClusterService clusterService;
    private RecoverySettings recoverySettings;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        environment = mock(Environment.class);
        contentRegistry = mock(NamedXContentRegistry.class);
        threadPool = mock(ThreadPool.class);
        transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        clusterService = mock(ClusterService.class);
        recoverySettings = mock(RecoverySettings.class);
        plugin1 = mock(RepositoryPlugin.class);
        plugin2 = mock(RepositoryPlugin.class);
        factory = mock(Repository.Factory.class);
        repoPlugins.add(plugin1);
        repoPlugins.add(plugin2);
        when(environment.settings()).thenReturn(Settings.EMPTY);
        nodeClient = mock(NodeClient.class);
    }

    public void testCanRegisterTwoRepositoriesWithDifferentTypes() {
        when(
            plugin1.getRepositories(
                eq(environment),
                eq(contentRegistry),
                eq(clusterService),
                eq(MockBigArrays.NON_RECYCLING_INSTANCE),
                eq(recoverySettings),
                any(RepositoriesMetrics.class)
            )
        ).thenReturn(Collections.singletonMap("type1", factory));
        when(
            plugin2.getRepositories(
                eq(environment),
                eq(contentRegistry),
                eq(clusterService),
                eq(MockBigArrays.NON_RECYCLING_INSTANCE),
                eq(recoverySettings),
                any(RepositoriesMetrics.class)
            )
        ).thenReturn(Collections.singletonMap("type2", factory));

        // Would throw
        new RepositoriesModule(
            environment,
            repoPlugins,
            nodeClient,
            threadPool,
            mock(ClusterService.class),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            contentRegistry,
            recoverySettings,
            TelemetryProvider.NOOP
        );
    }

    public void testCannotRegisterTwoRepositoriesWithSameTypes() {
        when(
            plugin1.getRepositories(
                eq(environment),
                eq(contentRegistry),
                eq(clusterService),
                eq(MockBigArrays.NON_RECYCLING_INSTANCE),
                eq(recoverySettings),
                any(RepositoriesMetrics.class)
            )
        ).thenReturn(Collections.singletonMap("type1", factory));
        when(
            plugin2.getRepositories(
                eq(environment),
                eq(contentRegistry),
                eq(clusterService),
                eq(MockBigArrays.NON_RECYCLING_INSTANCE),
                eq(recoverySettings),
                any(RepositoriesMetrics.class)
            )
        ).thenReturn(Collections.singletonMap("type1", factory));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new RepositoriesModule(
                environment,
                repoPlugins,
                nodeClient,
                threadPool,
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                contentRegistry,
                recoverySettings,
                TelemetryProvider.NOOP
            )
        );

        assertEquals("Repository type [type1] is already registered", ex.getMessage());
    }

    public void testCannotRegisterTwoInternalRepositoriesWithSameTypes() {
        when(plugin1.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings)).thenReturn(
            Collections.singletonMap("type1", factory)
        );
        when(plugin2.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings)).thenReturn(
            Collections.singletonMap("type1", factory)
        );

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new RepositoriesModule(
                environment,
                repoPlugins,
                nodeClient,
                threadPool,
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                contentRegistry,
                recoverySettings,
                TelemetryProvider.NOOP
            )
        );

        assertEquals("Internal repository type [type1] is already registered", ex.getMessage());
    }

    public void testCannotRegisterNormalAndInternalRepositoriesWithSameTypes() {
        when(
            plugin1.getRepositories(
                eq(environment),
                eq(contentRegistry),
                eq(clusterService),
                eq(MockBigArrays.NON_RECYCLING_INSTANCE),
                eq(recoverySettings),
                any(RepositoriesMetrics.class)
            )
        ).thenReturn(Collections.singletonMap("type1", factory));
        when(plugin2.getInternalRepositories(environment, contentRegistry, clusterService, recoverySettings)).thenReturn(
            Collections.singletonMap("type1", factory)
        );

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new RepositoriesModule(
                environment,
                repoPlugins,
                nodeClient,
                threadPool,
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                contentRegistry,
                recoverySettings,
                TelemetryProvider.NOOP
            )
        );

        assertEquals("Internal repository type [type1] is already registered as a non-internal repository", ex.getMessage());
    }
}
