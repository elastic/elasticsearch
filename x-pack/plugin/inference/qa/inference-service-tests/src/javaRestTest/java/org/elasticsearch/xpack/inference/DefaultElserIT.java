/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.inference.action.task.DefaultEndpoints;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.hamcrest.Matchers.hasSize;

public class DefaultElserIT extends InferenceBaseRestTest {

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(DefaultElserIT.class.getSimpleName());
    }

    @After
    public void tearDown() {
        threadPool.close();
    }
    @SuppressWarnings("unchecked")
    public void testInferCreatesDefaultElser() throws IOException {

        expectThrows(ResponseException.class, () -> getModel(DefaultEndpoints.DEFAULT_ELSER));

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");
        var results = infer(DefaultEndpoints.DEFAULT_ELSER, TaskType.SPARSE_EMBEDDING, inputs, queryParams);
        var embeddings = (List<Map<String, Object>>) results.get("sparse_embedding");
        assertThat(results.toString(), embeddings, hasSize(2));

        var modelMap = getModel(DefaultEndpoints.DEFAULT_ELSER);
        assertEquals(DefaultEndpoints.DEFAULT_ELSER, modelMap.get("inference_id"));
        assertEquals("elser", modelMap.get("service"));
        assertEquals(TaskType.SPARSE_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
        var serviceSettings = (Map<String, Object>) modelMap.get("service_settings");
        assertEquals(1, serviceSettings.get("num_threads"));
        assertThat(
            serviceSettings.toString(),
            serviceSettings.get("adaptive_allocations"),
            Matchers.is(Map.of("enabled", true, "min_number_of_allocations", 1))
        );
    }

    @SuppressWarnings("unchecked")
    public void testInferMultipleInferences() throws IOException {

        expectThrows(ResponseException.class, () -> getModel(DefaultEndpoints.DEFAULT_ELSER));

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");

        EsExecutors.newFixed(
            "f",
            1,
            1000,
            daemonThreadFactory("f"),

            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        )

        int numRequests = 10;
        for (int i=0; i<numRequests; i++) {
            threadPool.executor("foo").execute(() -> {
                var results = infer(DefaultEndpoints.DEFAULT_ELSER, TaskType.SPARSE_EMBEDDING, inputs, queryParams);
                var embeddings = (List<Map<String, Object>>) results.get("sparse_embedding");
                assertThat(results.toString(), embeddings, hasSize(2));
            });
        }
    }
}
