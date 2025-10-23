/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;

public class AuthorizationTaskExecutorIT extends ESSingleNodeTestCase {

    private ModelRegistry modelRegistry;
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private String gatewayUrl;

    @Before
    public void createComponents() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
        webServer.start();
        gatewayUrl = getUrl(webServer);
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        node().injector().getInstance(ClusterState.class);
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testCreateEndpoints() {
        var executor = new AuthorizationTaskExecutor();
    }
}
