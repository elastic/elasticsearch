/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.not;

public class QueryLoggingTemplateRegistryTests extends ESTestCase {

    private ThreadPool threadPool;

    @After
    public void stopPool() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    public void testDisabledDoesNotAddTemplates() {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        Settings settings = Settings.builder().put(QueryLoggingTemplateRegistry.QUERY_LOGGING_REGISTRY_ENABLED.getKey(), false).build();
        QueryLoggingTemplateRegistry registry = new QueryLoggingTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY
        );
        assertThat(registry.getComposableTemplateConfigs(), anEmptyMap());
        assertThat(registry.getComponentTemplateConfigs(), anEmptyMap());
    }

    public void testEnabledAddsTemplates() {
        threadPool = new TestThreadPool(getClass().getName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        QueryLoggingTemplateRegistry registry = new QueryLoggingTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            new NoOpClient(threadPool),
            NamedXContentRegistry.EMPTY
        );
        assertThat(registry.getComposableTemplateConfigs(), not(anEmptyMap()));
        assertThat(registry.getComponentTemplateConfigs(), not(anEmptyMap()));
    }
}
