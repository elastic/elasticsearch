/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;

public class AuthorizationTaskExecutorIT extends ESSingleNodeTestCase {

    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;

    private ModelRegistry modelRegistry;
    private ThreadPool threadPool;

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
    }

    @Before
    public void createComponents() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testCreateEndpoints() {
        var executor = new AuthorizationTaskExecutor();
    }
}
