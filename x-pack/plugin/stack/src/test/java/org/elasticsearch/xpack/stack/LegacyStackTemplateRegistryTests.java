/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.junit.After;
import org.junit.Before;

public class LegacyStackTemplateRegistryTests extends ESTestCase {
    private LegacyStackTemplateRegistry registry;
    private ThreadPool threadPool;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        Client client = new NoOpClient(threadPool);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new LegacyStackTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatTemplatesAreDeprecated() {
        for (ComposableIndexTemplate it : registry.getComposableTemplateConfigs().values()) {
            assertTrue(it.isDeprecated());
        }
        for (LifecyclePolicy ilm : registry.getLifecyclePolicies()) {
            assertTrue(ilm.isDeprecated());
        }
        for (ComponentTemplate ct : registry.getComponentTemplateConfigs().values()) {
            assertTrue(ct.deprecated());
        }
        registry.getIngestPipelines()
            .stream()
            .map(ipc -> new PipelineConfiguration(ipc.getId(), ipc.loadConfig(), XContentType.JSON))
            .map(PipelineConfiguration::getConfig)
            .forEach(p -> assertTrue((Boolean) p.get("deprecated")));
    }

}
