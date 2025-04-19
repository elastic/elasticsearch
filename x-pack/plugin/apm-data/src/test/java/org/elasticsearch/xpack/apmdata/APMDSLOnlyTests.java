/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class APMDSLOnlyTests extends ESTestCase {
    private APMIndexTemplateRegistry apmIndexTemplateRegistry;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Set.of(APMPlugin.APM_DATA_REGISTRY_ENABLED).stream())
                .collect(Collectors.toSet())
        );

        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("node", "node");
        Settings additionalSettings = Settings.builder().put(DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME, true).build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            discoveryNode,
            additionalSettings,
            clusterSettings
        );

        apmIndexTemplateRegistry = new APMIndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.mustExecuteFirst()
        );
        apmIndexTemplateRegistry.setEnabled(true);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testNoILMComponentTemplateInstalled() throws Exception {
        for (Map.Entry<String, ComponentTemplate> entry : apmIndexTemplateRegistry.getComponentTemplateConfigs().entrySet()) {
            final String name = entry.getKey();
            final int atIndex = name.lastIndexOf('@');
            assertThat(atIndex, not(equalTo(-1)));
            // No ILM templates should have been loaded
            assertThat(name.substring(atIndex + 1), not(equalTo("ilm")));
        }
    }
}
