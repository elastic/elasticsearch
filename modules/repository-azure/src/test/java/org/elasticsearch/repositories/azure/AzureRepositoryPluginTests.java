/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.cluster.node.DiscoveryNode.STATELESS_ENABLED_SETTING_NAME;
import static org.hamcrest.Matchers.equalTo;

public class AzureRepositoryPluginTests extends ESTestCase {

    public void testRepositoryAzureMaxThreads() {
        final boolean isServerless = randomBoolean();
        final var settings = Settings.builder().put("node.name", getTestName()).put(STATELESS_ENABLED_SETTING_NAME, isServerless).build();

        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(
                settings,
                MeterRegistry.NOOP,
                (settings1, allocatedProcessors) -> Map.of(),
                AzureRepositoryPlugin.executorBuilder(settings)
            );

            assertThat(
                threadPool.info(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME).getMax(),
                equalTo(isServerless ? ThreadPool.getMaxSnapshotThreadPoolSize(EsExecutors.allocatedProcessors(settings)) + 5 : 5)
            );
        } finally {
            terminate(threadPool);
        }
    }
}
