/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class APMPluginTests extends ESTestCase {
    private APMPlugin apmPlugin;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void createPlugin() {
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Set.of(APMPlugin.APM_DATA_REGISTRY_ENABLED).stream())
                .collect(Collectors.toSet())
        );
        threadPool = new TestThreadPool(this.getClass().getName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        apmPlugin = new APMPlugin(Settings.builder().put(XPackSettings.APM_DATA_ENABLED.getKey(), true).build());
    }

    private void createComponents() {
        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.settings()).thenReturn(Settings.builder().build());
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.clusterService()).thenReturn(clusterService);
        when(services.threadPool()).thenReturn(threadPool);
        when(services.environment()).thenReturn(mockEnvironment);
        apmPlugin.createComponents(services);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        apmPlugin.close();
        threadPool.shutdownNow();
    }

    public void testRegistryEnabledSetting() throws Exception {
        createComponents();

        // By default, the registry is enabled.
        assertTrue(apmPlugin.registry.get().isEnabled());

        // The registry can be disabled/enabled dynamically.
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(APMPlugin.APM_DATA_REGISTRY_ENABLED.getKey(), false).build());
        assertFalse(apmPlugin.registry.get().isEnabled());
    }

    public void testDisablingPluginDisablesRegistry() throws Exception {
        apmPlugin = new APMPlugin(Settings.builder().put(XPackSettings.APM_DATA_ENABLED.getKey(), false).build());
        createComponents();

        // The plugin is disabled, so the registry is disabled too.
        assertFalse(apmPlugin.registry.get().isEnabled());

        // The registry can not be enabled dynamically when the plugin is disabled.
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(APMPlugin.APM_DATA_REGISTRY_ENABLED.getKey(), true).build());
        assertFalse(apmPlugin.registry.get().isEnabled());
    }
}
