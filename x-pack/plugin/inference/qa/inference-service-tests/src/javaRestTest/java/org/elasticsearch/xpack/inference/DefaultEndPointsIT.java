/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class DefaultEndPointsIT extends InferenceBaseRestTest {

    private TestThreadPool threadPool;

    @Before
    public void setupTest() throws Exception {
        threadPool = new TestThreadPool(DefaultEndPointsIT.class.getSimpleName());

        Request loggingSettings = new Request("PUT", "_cluster/settings");
        loggingSettings.setJsonEntity("""
            {"persistent" : {
                    "logger.org.elasticsearch.xpack.ml.packageloader" : "DEBUG"
                }}""");
        client().performRequest(loggingSettings);
        initInferenceIndices();
        ensureNoInitializingShards();
    }

    @After
    public void tearDown() throws Exception {
        threadPool.close();
        super.tearDown();
    }

    public void testGet() throws IOException {
        var elserModel = getModel(ElasticsearchInternalService.DEFAULT_ELSER_ID);
        assertDefaultElserConfig(elserModel);

        var e5Model = getModel(ElasticsearchInternalService.DEFAULT_E5_ID);
        assertDefaultE5Config(e5Model);

        var rerankModel = getModel(ElasticsearchInternalService.DEFAULT_RERANK_ID);
        assertDefaultRerankConfig(rerankModel);
    }

    public void testDefaultModels() throws IOException {
        var elserModel = getModel(ElasticsearchInternalService.DEFAULT_ELSER_ID);
        assertDefaultElserConfig(elserModel);

        var e5Model = getModel(ElasticsearchInternalService.DEFAULT_E5_ID);
        assertDefaultE5Config(e5Model);

        var rerankModel = getModel(ElasticsearchInternalService.DEFAULT_RERANK_ID);
        assertDefaultRerankConfig(rerankModel);

        putModel("my-model", mockCompletionServiceModelConfig(TaskType.SPARSE_EMBEDDING, "streaming_completion_test_service"));
        var registeredModels = getMinimalConfigs();
        assertThat(registeredModels.size(), is(1));
        assertTrue(registeredModels.containsKey("my-model"));
        assertFalse(registeredModels.containsKey(ElasticsearchInternalService.DEFAULT_E5_ID));
        assertFalse(registeredModels.containsKey(ElasticsearchInternalService.DEFAULT_ELSER_ID));
        assertFalse(registeredModels.containsKey(ElasticsearchInternalService.DEFAULT_RERANK_ID));
    }

    @SuppressWarnings("unchecked")
    public void testInferDeploysDefaultElser() throws IOException {
        var model = getModel(ElasticsearchInternalService.DEFAULT_ELSER_ID);
        assertDefaultElserConfig(model);

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");
        var results = infer(ElasticsearchInternalService.DEFAULT_ELSER_ID, TaskType.SPARSE_EMBEDDING, inputs, queryParams);
        var embeddings = (List<Map<String, Object>>) results.get(SparseEmbeddingResults.SPARSE_EMBEDDING);
        assertThat(results.toString(), embeddings, hasSize(2));
    }

    @SuppressWarnings("unchecked")
    private static void assertDefaultElserConfig(Map<String, Object> modelConfig) {
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.DEFAULT_ELSER_ID, modelConfig.get("inference_id"));
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.NAME, modelConfig.get("service"));
        assertEquals(modelConfig.toString(), TaskType.SPARSE_EMBEDDING.toString(), modelConfig.get("task_type"));

        var serviceSettings = (Map<String, Object>) modelConfig.get("service_settings");
        assertThat(modelConfig.toString(), serviceSettings.get("model_id"), is(oneOf(".elser_model_2", ".elser_model_2_linux-x86_64")));
        assertEquals(modelConfig.toString(), 1, serviceSettings.get("num_threads"));

        var adaptiveAllocations = (Map<String, Object>) serviceSettings.get("adaptive_allocations");
        assertThat(
            modelConfig.toString(),
            adaptiveAllocations,
            Matchers.is(Map.of("enabled", true, "min_number_of_allocations", 0, "max_number_of_allocations", 32))
        );
        assertDefaultChunkingSettings(modelConfig);
    }

    @SuppressWarnings("unchecked")
    public void testInferDeploysDefaultE5() throws IOException {
        var model = getModel(ElasticsearchInternalService.DEFAULT_E5_ID);
        assertDefaultE5Config(model);

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");
        var results = infer(ElasticsearchInternalService.DEFAULT_E5_ID, TaskType.TEXT_EMBEDDING, inputs, queryParams);
        var embeddings = (List<Map<String, Object>>) results.get(DenseEmbeddingFloatResults.TEXT_EMBEDDING);
        assertThat(results.toString(), embeddings, hasSize(2));
    }

    @SuppressWarnings("unchecked")
    private static void assertDefaultE5Config(Map<String, Object> modelConfig) {
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.DEFAULT_E5_ID, modelConfig.get("inference_id"));
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.NAME, modelConfig.get("service"));
        assertEquals(modelConfig.toString(), TaskType.TEXT_EMBEDDING.toString(), modelConfig.get("task_type"));

        var serviceSettings = (Map<String, Object>) modelConfig.get("service_settings");
        assertThat(
            modelConfig.toString(),
            serviceSettings.get("model_id"),
            is(oneOf(".multilingual-e5-small", ".multilingual-e5-small_linux-x86_64"))
        );
        assertEquals(modelConfig.toString(), 1, serviceSettings.get("num_threads"));

        var adaptiveAllocations = (Map<String, Object>) serviceSettings.get("adaptive_allocations");
        assertThat(
            modelConfig.toString(),
            adaptiveAllocations,
            Matchers.is(Map.of("enabled", true, "min_number_of_allocations", 0, "max_number_of_allocations", 32))
        );
        assertDefaultChunkingSettings(modelConfig);
    }

    @SuppressWarnings("unchecked")
    public void testInferDeploysDefaultRerank() throws IOException {
        var model = getModel(ElasticsearchInternalService.DEFAULT_RERANK_ID);
        assertDefaultRerankConfig(model);

        var inputs = List.of("Hello World", "Goodnight moon");
        var query = "but why";
        var queryParams = Map.of("timeout", "120s");
        var results = infer(ElasticsearchInternalService.DEFAULT_RERANK_ID, TaskType.RERANK, inputs, query, queryParams);
        var embeddings = (List<Map<String, Object>>) results.get("rerank");
        assertThat(results.toString(), embeddings, hasSize(2));
    }

    @SuppressWarnings("unchecked")
    private static void assertDefaultRerankConfig(Map<String, Object> modelConfig) {
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.DEFAULT_RERANK_ID, modelConfig.get("inference_id"));
        assertEquals(modelConfig.toString(), ElasticsearchInternalService.NAME, modelConfig.get("service"));
        assertEquals(modelConfig.toString(), TaskType.RERANK.toString(), modelConfig.get("task_type"));

        var serviceSettings = (Map<String, Object>) modelConfig.get("service_settings");
        assertThat(modelConfig.toString(), serviceSettings.get("model_id"), is(".rerank-v1"));
        assertEquals(modelConfig.toString(), 1, serviceSettings.get("num_threads"));

        var adaptiveAllocations = (Map<String, Object>) serviceSettings.get("adaptive_allocations");
        assertThat(
            modelConfig.toString(),
            adaptiveAllocations,
            Matchers.is(Map.of("enabled", true, "min_number_of_allocations", 0, "max_number_of_allocations", 32))
        );

        var chunkingSettings = (Map<String, Object>) modelConfig.get("chunking_settings");
        assertNull(chunkingSettings);
        var taskSettings = (Map<String, Object>) modelConfig.get("task_settings");
        assertThat(modelConfig.toString(), taskSettings, Matchers.is(Map.of("return_documents", true)));
    }

    @SuppressWarnings("unchecked")
    private static void assertDefaultChunkingSettings(Map<String, Object> modelConfig) {
        var chunkingSettings = (Map<String, Object>) modelConfig.get("chunking_settings");
        assertThat(
            modelConfig.toString(),
            chunkingSettings,
            Matchers.is(Map.of("strategy", "sentence", "max_chunk_size", 250, "sentence_overlap", 1))
        );
    }

    public void testMultipleInferencesTriggeringDownloadAndDeploy() throws InterruptedException, IOException {
        var initialEndpointId = "initial-model";
        // Creating an inference endpoint to force the backing indices to be created to reduce the likelihood of the test failing
        // because it's trying to interact with the indices while they're being created.
        putModel(initialEndpointId, mockCompletionServiceModelConfig(TaskType.SPARSE_EMBEDDING, "streaming_completion_test_service"));
        // delete model so it doesn't affect other tests
        deleteModel(initialEndpointId);

        int numParallelRequests = 4;
        var latch = new CountDownLatch(numParallelRequests);
        var errors = new ArrayList<Exception>();
        var successCount = new AtomicInteger(0);

        var listener = new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                successCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                errors.add(exception);
                latch.countDown();
            }
        };

        var inputs = List.of("Hello World", "Goodnight moon");
        var queryParams = Map.of("timeout", "120s");
        for (int i = 0; i < numParallelRequests; i++) {
            var request = createInferenceRequest(
                Strings.format("_inference/%s", ElasticsearchInternalService.DEFAULT_ELSER_ID),
                inputs,
                null,
                queryParams
            );
            client().performRequestAsync(request, listener);
        }

        latch.await();
        // Filter out transient shard unavailability errors on .ml-inference-* indices. These can occur when
        // multiple concurrent requests race to initialize the ML model storage index during the first deployment.
        var significantErrors = errors.stream().filter(e -> isTransientMlInferenceIndexError(e) == false).toList();
        assertThat("Received non-transient errors", significantErrors, empty());
        assertThat("Expected at least one inference request to succeed", successCount.get(), greaterThan(0));
        assertElserDeploymentStarted();
    }

    /**
     * Asserts that the ELSER deployment is in a started or fully-allocated state by querying
     * {@code _ml/trained_models/<model_id>/_stats}. Called after inference requests complete to
     * confirm the deployment is healthy and ready to serve requests.
     *
     * The response json of getTrainedModelStats is:
     * {
     *   "count": 2,
     *   "trained_model_stats": [
     *     {
     *       "model_id": ".elser_model_2_linux-x86_64",
     *       "... other fields ...",
     *       "deployment_stats": {
     *         "... other fields ...",
     *         "state": "started",
     *         "allocation_status": {
     *           "allocation_count": 1,
     *           "target_allocation_count": 1,
     *           "state": "fully_allocated"
     *         },
     *         "... other fields ...",
     *       }
     *     },
     *     "... other trained model stats ...",
     *   ]
     * }
     */
    @SuppressWarnings("unchecked")
    private void assertElserDeploymentStarted() throws IOException {
        var elserConfig = getModel(ElasticsearchInternalService.DEFAULT_ELSER_ID);
        var serviceSettings = (Map<String, Object>) elserConfig.get("service_settings");
        var mlModelId = (String) serviceSettings.get("model_id");

        var statsResponse = getTrainedModelStats(mlModelId);
        var trainedModelStats = (List<Map<String, Object>>) statsResponse.get("trained_model_stats");
        assertFalse(statsResponse.toString(), trainedModelStats.isEmpty());

        var state = (String) XContentMapValues.extractValue("deployment_stats.allocation_status.state", trainedModelStats.get(0));
        assertThat(statsResponse.toString(), state, is(oneOf("started", "fully_allocated")));
    }

    /**
     * Returns true if the exception is a transient 503 caused by a not-yet-initialized shard on a .ml-inference-*
     * index. This happens when concurrent requests simultaneously trigger a built-in model deployment and one of
     * them searches the ML inference index while another is in the process of creating it.
     */
    private static boolean isTransientMlInferenceIndexError(Exception e) {
        return e instanceof ResponseException re
            && re.getResponse().getStatusLine().getStatusCode() == RestStatus.SERVICE_UNAVAILABLE.getStatus()
            && e.getMessage().contains("no_shard_available_action_exception")
            && e.getMessage().contains(".ml-inference-");
    }
}
