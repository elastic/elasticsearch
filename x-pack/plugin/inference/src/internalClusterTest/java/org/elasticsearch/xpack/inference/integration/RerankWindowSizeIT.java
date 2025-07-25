/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.inference.action.GetRerankerAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RerankWindowSizeIT extends ESIntegTestCase {

    @Before
    public void setup() throws Exception {
        ModelRegistry modelRegistry = internalCluster().getCurrentMasterNodeInstance(ModelRegistry.class);
        Utils.storeRerankModel("rerank-endpoint", modelRegistry);
        Utils.storeSparseModel("sparse-endpoint", modelRegistry);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class);
    }

    public void testRerankWindowSizeAction() {
        var response = client().execute(GetRerankerAction.INSTANCE, new GetRerankerAction.Request("rerank-endpoint")).actionGet();
        assertEquals(333, response.getWindowSize());
    }

    public void testActionNotARerankder() {
        var e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GetRerankerAction.INSTANCE, new GetRerankerAction.Request("sparse-endpoint")).actionGet()
        );
        assertThat(e.getMessage(), containsString("Inference endpoint [sparse-endpoint] is not a reranker"));
    }
}
