/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

public abstract class AnalyticsTestCase extends ESSingleNodeTestCase {

    @Before
    public void setupTemplateRegistry() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        if (isTemplateRegistrySetup(clusterService().state())) {
            latch.countDown();
        } else {
            clusterService().addListener(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    if (isTemplateRegistrySetup(event.state())) {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
    }

    @Override
    public void tearDown() throws Exception {
        clusterService().removeListener(analyticsTemplateRegistry());
        super.tearDown();
        clusterService().addListener(analyticsTemplateRegistry());
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(XPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class, TestPlugin.class);
    }

    protected ClusterService clusterService() {
        return getInstanceFromNode(ClusterService.class);
    }

    protected AnalyticsTemplateRegistry analyticsTemplateRegistry() {
        return getInstanceFromNode(AnalyticsTemplateRegistry.class);
    }

    protected AnalyticsCollectionService analyticsCollectionService() {
        return getInstanceFromNode(AnalyticsCollectionService.class);
    }

    private boolean isTemplateRegistrySetup(ClusterState state) {
        return state.metadata().templatesV2().containsKey(AnalyticsTemplateRegistry.EVENT_DATA_STREAM_TEMPLATE_NAME);
    }

    /**
     * Mock plugin used to instantiate analytics tests requirement.
     */
    public static class TestPlugin extends Plugin {
        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationService allocationService
        ) {
            AnalyticsTemplateRegistry analyticsTemplateRegistry = new AnalyticsTemplateRegistry(
                clusterService,
                threadPool,
                client,
                xContentRegistry
            );
            analyticsTemplateRegistry.initialize();

            AnalyticsCollectionService analyticsCollectionService = new AnalyticsCollectionService(
                client,
                clusterService,
                indexNameExpressionResolver
            );
            return Arrays.asList(analyticsTemplateRegistry, analyticsCollectionService);
        }
    }
}
