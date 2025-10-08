/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ModelRegistryTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.integration.ModelRegistryIT.createModel;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL;

public class ModelRegistryEisBase extends ESSingleNodeTestCase {
    protected static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    protected static final MockWebServer webServer = new MockWebServer();

    protected ModelRegistry modelRegistry;
    private String eisUrl;

    public ModelRegistryEisBase() {}

    public ModelRegistryEisBase(String eisUrl) {
        this.eisUrl = eisUrl;
    }

    @BeforeClass
    public static void init() throws Exception {
        webServer.start();
    }

    @AfterClass
    public static void shutdown() {
        webServer.close();
    }

    @Before
    public void createComponents() {
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        modelRegistry.clearDefaultIds();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(ELASTIC_INFERENCE_SERVICE_URL.getKey(), getEisUrl()).build();
    }

    private String getEisUrl() {
        return eisUrl != null ? eisUrl : getUrl(webServer);
    }

    protected void initializeModels() {
        var service = "foo";
        var sparseAndTextEmbeddingModels = new ArrayList<Model>();
        sparseAndTextEmbeddingModels.add(createModel("sparse-1", TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel("sparse-2", TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel("sparse-3", TaskType.SPARSE_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel("embedding-1", TaskType.TEXT_EMBEDDING, service));
        sparseAndTextEmbeddingModels.add(createModel("embedding-2", TaskType.TEXT_EMBEDDING, service));

        for (var model : sparseAndTextEmbeddingModels) {
            ModelRegistryTests.assertStoreModel(modelRegistry, model);
        }
    }
}
