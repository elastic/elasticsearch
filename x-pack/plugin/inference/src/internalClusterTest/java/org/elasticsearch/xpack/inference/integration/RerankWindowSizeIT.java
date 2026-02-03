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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestRerankingServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
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
        var response = client().execute(GetRerankerWindowSizeAction.INSTANCE, new GetRerankerWindowSizeAction.Request("rerank-endpoint"))
            .actionGet();
        assertEquals(TestRerankingServiceExtension.RERANK_WINDOW_SIZE, response.getWindowSize());
    }

    public void testActionNotAReranker() {
        var e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GetRerankerWindowSizeAction.INSTANCE, new GetRerankerWindowSizeAction.Request("sparse-endpoint"))
                .actionGet()
        );
        assertThat(e.getMessage(), containsString("Inference endpoint [sparse-endpoint] does not have the rerank task type"));
    }
}
