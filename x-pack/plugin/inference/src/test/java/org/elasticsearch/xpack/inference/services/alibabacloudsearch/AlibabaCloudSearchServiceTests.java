/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.alibabacloudsearch.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsTaskSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws IOException {
        try (var service = new AlibabaCloudSearchService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool))) {
            ActionListener<Model> modelVerificationListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(AlibabaCloudSearchEmbeddingsModel.class));

                var embeddingsModel = (AlibabaCloudSearchEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().modelId(), is("service_id"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getHost(), is("host"));
                assertThat(embeddingsModel.getServiceSettings().getCommonSettings().getWorkspaceName(), is("default"));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), is("secret"));
            }, e -> fail("Model parsing should have succeeded " + e.getMessage()));

            service.parseRequestConfig(
                "id",
                TaskType.TEXT_EMBEDDING,
                getRequestConfigMap(
                    AlibabaCloudSearchEmbeddingsServiceSettingsTests.getServiceSettingsMap("service_id", "host", "default"),
                    AlibabaCloudSearchEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                    getSecretSettingsMap("secret")
                ),
                modelVerificationListener
            );
        }
    }

    public void testCheckModelConfig() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool)) {
            @Override
            public void doInfer(
                Model model,
                InferenceInputs inputs,
                Map<String, Object> taskSettings,
                InputType inputType,
                TimeValue timeout,
                ActionListener<InferenceServiceResults> listener
            ) {
                InferenceTextEmbeddingFloatResults results = new InferenceTextEmbeddingFloatResults(
                    List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { -0.028680f, 0.022033f }))
                );

                listener.onResponse(results);
            }
        }) {
            Map<String, Object> serviceSettingsMap = new HashMap<>();
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
            serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

            Map<String, Object> taskSettingsMap = new HashMap<>();

            Map<String, Object> secretSettingsMap = new HashMap<>();
            secretSettingsMap.put("api_key", "secret");

            var model = AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "service",
                TaskType.TEXT_EMBEDDING,
                serviceSettingsMap,
                taskSettingsMap,
                secretSettingsMap
            );
            PlainActionFuture<Model> listener = new PlainActionFuture<>();
            service.checkModelConfig(model, listener);
            var result = listener.actionGet(TIMEOUT);

            Map<String, Object> expectedServiceSettingsMap = new HashMap<>();
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
            expectedServiceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
            expectedServiceSettingsMap.put(ServiceFields.SIMILARITY, "DOT_PRODUCT");
            expectedServiceSettingsMap.put(ServiceFields.DIMENSIONS, 2);

            Map<String, Object> expectedTaskSettingsMap = new HashMap<>();

            Map<String, Object> expectedSecretSettingsMap = new HashMap<>();
            expectedSecretSettingsMap.put("api_key", "secret");

            var expectedModel = AlibabaCloudSearchEmbeddingsModelTests.createModel(
                "service",
                TaskType.TEXT_EMBEDDING,
                expectedServiceSettingsMap,
                expectedTaskSettingsMap,
                expectedSecretSettingsMap
            );

            MatcherAssert.assertThat(result, is(expectedModel));
        }
    }

    public void testChunkedInfer_Batches() throws IOException {
        var input = List.of("foo", "bar");

        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new AlibabaCloudSearchService(senderFactory, createWithEmptySettings(threadPool))) {
            Map<String, Object> serviceSettingsMap = new HashMap<>();
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, "service_id");
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.HOST, "host");
            serviceSettingsMap.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, "default");
            serviceSettingsMap.put(ServiceFields.DIMENSIONS, 1536);

            Map<String, Object> taskSettingsMap = new HashMap<>();

            Map<String, Object> secretSettingsMap = new HashMap<>();
            secretSettingsMap.put("api_key", "secret");

            var model = new AlibabaCloudSearchEmbeddingsModel(
                "service",
                TaskType.TEXT_EMBEDDING,
                AlibabaCloudSearchUtils.SERVICE_NAME,
                serviceSettingsMap,
                taskSettingsMap,
                secretSettingsMap,
                null
            ) {
                public ExecutableAction accept(
                    AlibabaCloudSearchActionVisitor visitor,
                    Map<String, Object> taskSettings,
                    InputType inputType
                ) {
                    return (inferenceInputs, timeout, listener) -> {
                        InferenceTextEmbeddingFloatResults results = new InferenceTextEmbeddingFloatResults(
                            List.of(
                                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.0123f, -0.0123f }),
                                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.0456f, -0.0456f })
                            )
                        );

                        listener.onResponse(results);
                    };
                }
            };

            PlainActionFuture<List<ChunkedInferenceServiceResults>> listener = new PlainActionFuture<>();
            service.chunkedInfer(
                model,
                null,
                input,
                new HashMap<>(),
                InputType.INGEST,
                new ChunkingOptions(null, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var results = listener.actionGet(TIMEOUT);
            assertThat(results, hasSize(2));

            // first result
            {
                assertThat(results.get(0), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(0);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(input.get(0), floatResult.chunks().get(0).matchedText());
                assertTrue(Arrays.equals(new float[] { 0.0123f, -0.0123f }, floatResult.chunks().get(0).embedding()));
            }

            // second result
            {
                assertThat(results.get(1), CoreMatchers.instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
                var floatResult = (InferenceChunkedTextEmbeddingFloatResults) results.get(1);
                assertThat(floatResult.chunks(), hasSize(1));
                assertEquals(input.get(1), floatResult.chunks().get(0).matchedText());
                assertTrue(Arrays.equals(new float[] { 0.0456f, -0.0456f }, floatResult.chunks().get(0).embedding()));
            }
        }
    }

    private Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        var builtServiceSettings = new HashMap<>();
        builtServiceSettings.putAll(serviceSettings);
        builtServiceSettings.putAll(secretSettings);

        return new HashMap<>(
            Map.of(ModelConfigurations.SERVICE_SETTINGS, builtServiceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)
        );
    }
}
