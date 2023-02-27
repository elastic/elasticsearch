/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;

public class AnalyticsCollectionServiceTests extends ESSingleNodeTestCase {
    private static final int NUM_COLLECTIONS = 10;

    private AnalyticsCollectionService analyticsCollectionService;

    private ClusterService clusterService;

    @Before
    public void setup() throws Exception {
        clusterService = getInstanceFromNode(ClusterService.class);
        analyticsCollectionService = new AnalyticsCollectionService(client(), clusterService, indexNameExpressionResolver());

        for (int i = 0; i < NUM_COLLECTIONS; i++) {
            createDataStream(new AnalyticsCollection("collection_" + i).getEventDataStream());
        }
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            XPackPlugin.class,
            IndexLifecycle.class,
            DataStreamsPlugin.class,
            TestPlugin.class
        );
    }

    public void testGetExistingAnalyticsCollection() throws Exception {
        AnalyticsCollection analyticsCollection = awaitGetAnalyticsCollection("collection_1");
        assertThat(analyticsCollection.getName(), equalTo("collection_1"));
    }

    private IndexNameExpressionResolver indexNameExpressionResolver() {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
    }

    private void createDataStream(String dataStreamName) throws ExecutionException, InterruptedException {
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get();
    }

    private AnalyticsCollection awaitGetAnalyticsCollection(String collectionName) throws Exception {
        return  new ResponseAwaiter<String, AnalyticsCollection>(
            analyticsCollectionService::getAnalyticsCollection
        ).get(collectionName);
    }

    private static class ResponseAwaiter<T, R> {
        BiConsumer<T, ActionListener<R>> f;

        public ResponseAwaiter(BiConsumer<T, ActionListener<R>> f) {
            this.f = f;
        }

        public R get(T param) throws Exception {
            CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<R> resp = new AtomicReference<>(null);
            final AtomicReference<Exception> exc = new AtomicReference<>(null);

            f.accept(param, new ActionListener<R>() {
                @Override
                public void onResponse(R r) {
                    resp.set(r);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exc.set(e);
                    latch.countDown();
                }
            });

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            if (exc.get() != null) {
                throw exc.get();
            }
            assertNotNull(resp.get());
            return resp.get();
        }
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
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
            AnalyticsTemplateRegistry analyticsTemplateRegistry = new AnalyticsTemplateRegistry(clusterService, threadPool, client, xContentRegistry);
            analyticsTemplateRegistry.initialize();
            return Collections.singletonList(analyticsTemplateRegistry);
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return this.getClass().getCanonicalName();
        }
    }
}

