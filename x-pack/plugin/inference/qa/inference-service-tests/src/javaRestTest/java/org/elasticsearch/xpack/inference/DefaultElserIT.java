/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class DefaultElserIT extends InferenceBaseRestTest {

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(DefaultElserIT.class.getSimpleName());
    }

    @After
    public void tearDown() throws Exception {
        threadPool.close();
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testInferCreatesDefaultElser() throws IOException {
        assumeTrue("Default config requires a feature flag", DefaultElserFeatureFlag.isEnabled());
        var model = getModel(ElserInternalService.DEFAULT_ELSER_ID);
        assertDefaultElserConfig(model);

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");
        var results = infer(ElserInternalService.DEFAULT_ELSER_ID, TaskType.SPARSE_EMBEDDING, inputs, queryParams);
        var embeddings = (List<Map<String, Object>>) results.get("sparse_embedding");
        assertThat(results.toString(), embeddings, hasSize(2));
    }

    public void testScaleFrom0() {
        assumeTrue("Default config requires a feature flag", DefaultElserFeatureFlag.isEnabled());
        fail("how can we test this when we need to wait for the cooldown");
    }

    /*
    @SuppressWarnings("unchecked")
    public void testInferMultipleInferences() throws IOException {
        assumeTrue("Default config requires a feature flag", DefaultElserFeatureFlag.isEnabled());
        var model = getModel(ElserInternalService.DEFAULT_ELSER_ID);
        assertDefaultElserConfig(model);

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");

        EsExecutors.newFixed(
            DefaultElserIT.class.getSimpleName() + "-testthreads",
            1,
            1000,
            daemonThreadFactory(DefaultElserIT.class.getSimpleName()),
            threadPool.getThreadContext(),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );

        int numRequests = 10;
        for (int i = 0; i < numRequests; i++) {
            threadPool.executor("foo").execute(() -> {
                Map<String, Object> results = null;
                try {
                    results = infer(ElserInternalService.DEFAULT_ELSER_ID, TaskType.SPARSE_EMBEDDING, inputs, queryParams);
                } catch (IOException e) {
                    fail(e, "error inferring");
                }
                var embeddings = (List<Map<String, Object>>) results.get("sparse_embedding");
                assertThat(results.toString(), embeddings, hasSize(2));
            });
        }
    }
    */
    @SuppressWarnings("unchecked")
    private static void assertDefaultElserConfig(Map<String, Object> modelConfig) {
        assertEquals(modelConfig.toString(), ElserInternalService.DEFAULT_ELSER_ID, modelConfig.get("inference_id"));
        assertEquals(modelConfig.toString(), ElserInternalService.NAME, modelConfig.get("service"));
        assertEquals(modelConfig.toString(), TaskType.SPARSE_EMBEDDING.toString(), modelConfig.get("task_type"));

        var serviceSettings = (Map<String, Object>) modelConfig.get("service_settings");
        assertThat(modelConfig.toString(), serviceSettings.get("model_id"), is(oneOf(".elser_model_2", ".elser_model_2_linux-x86_64")));
        assertEquals(modelConfig.toString(), 1, serviceSettings.get("num_threads"));

        var adaptiveAllocations = (Map<String, Object>) serviceSettings.get("adaptive_allocations");
        assertThat(
            modelConfig.toString(),
            adaptiveAllocations,
            Matchers.is(Map.of("enabled", true, "min_number_of_allocations", 0, "max_number_of_allocations", 8))
        );
    }
}
