/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestRerankingServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.inference.InferencePlugin.INFERENCE_RESPONSE_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class Utils {

    public static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private Utils() {
        throw new UnsupportedOperationException("Utils is a utility class and should not be instantiated");
    }

    public static ClusterService mockClusterServiceEmpty() {
        return mockClusterService(Settings.EMPTY);
    }

    public static ClusterService mockClusterService(Settings settings) {
        var clusterService = mock(ClusterService.class);

        var registeredSettings = InferencePlugin.getInferenceSettings();

        var cSettings = new ClusterSettings(settings, registeredSettings);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);

        return clusterService;
    }

    public static ScalingExecutorBuilder[] inferenceUtilityExecutors() {
        return new ScalingExecutorBuilder[] {
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.utility_thread_pool"
            ),
            new ScalingExecutorBuilder(
                INFERENCE_RESPONSE_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.inference_response_thread_pool"
            ) };
    }

    public static void storeSparseModel(String inferenceId, ModelRegistry modelRegistry) throws Exception {
        Model model = new TestSparseInferenceServiceExtension.TestSparseModel(
            inferenceId,
            new TestSparseInferenceServiceExtension.TestServiceSettings("sparse_model", null, false)
        );
        storeModel(modelRegistry, model);
    }

    public static void storeDenseModel(
        String inferenceId,
        ModelRegistry modelRegistry,
        int dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) throws Exception {
        Model model = new TestDenseInferenceServiceExtension.TestDenseModel(
            inferenceId,
            new TestDenseInferenceServiceExtension.TestServiceSettings("dense_model", dimensions, similarityMeasure, elementType)
        );
        storeModel(modelRegistry, model);
    }

    public static void storeRerankModel(String inferenceId, ModelRegistry modelRegistry) throws Exception {
        Model model = new TestRerankingServiceExtension.TestRerankingModel(
            inferenceId,
            new TestRerankingServiceExtension.TestServiceSettings("rerank-model")
        );
        storeModel(modelRegistry, model);
    }

    public static void storeModel(ModelRegistry modelRegistry, Model model) throws Exception {
        PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
        modelRegistry.storeModel(model, listener, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        assertTrue(listener.actionGet(TimeValue.THIRTY_SECONDS));
    }

    public static Model getInvalidModel(String inferenceEntityId, String serviceName, TaskType taskType) {
        var mockConfigs = mock(ModelConfigurations.class);
        when(mockConfigs.getInferenceEntityId()).thenReturn(inferenceEntityId);
        when(mockConfigs.getService()).thenReturn(serviceName);
        when(mockConfigs.getTaskType()).thenReturn(taskType);

        var mockModel = mock(Model.class);
        when(mockModel.getInferenceEntityId()).thenReturn(inferenceEntityId);
        when(mockModel.getConfigurations()).thenReturn(mockConfigs);
        when(mockModel.getTaskType()).thenReturn(taskType);

        return mockModel;
    }

    public static Model getInvalidModel(String inferenceEntityId, String serviceName) {
        return getInvalidModel(inferenceEntityId, serviceName, TaskType.TEXT_EMBEDDING);
    }

    public static SimilarityMeasure randomSimilarityMeasure() {
        return randomFrom(SimilarityMeasure.values());
    }

    public record PersistedConfig(Map<String, Object> config, Map<String, Object> secrets) {}

    public static PersistedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {

        var persistedConfigMap = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);
        persistedConfigMap.config.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return persistedConfigMap;
    }

    public static PersistedConfig getPersistedConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        var secrets = secretSettings == null ? null : new HashMap<String, Object>(Map.of(ModelSecrets.SECRET_SETTINGS, secretSettings));

        return new PersistedConfig(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            secrets
        );
    }

    public static PersistedConfig getPersistedConfigMap(Map<String, Object> serviceSettings) {
        return Utils.getPersistedConfigMap(serviceSettings, new HashMap<>(), null);
    }

    public static PersistedConfig getPersistedConfigMap(Map<String, Object> serviceSettings, Map<String, Object> taskSettings) {
        return new PersistedConfig(
            new HashMap<>(Map.of(ModelConfigurations.SERVICE_SETTINGS, serviceSettings, ModelConfigurations.TASK_SETTINGS, taskSettings)),
            null
        );
    }

    public static Map<String, Object> getRequestConfigMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, taskSettings, secretSettings);
        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
    }

    public static Map<String, Object> getRequestConfigMap(
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

    public static Map<String, Object> buildExpectationCompletions(List<String> completions) {
        return Map.of(
            ChatCompletionResults.COMPLETION,
            completions.stream().map(completion -> Map.of(ChatCompletionResults.Result.RESULT, completion)).collect(Collectors.toList())
        );
    }

    public static ActionListener<Model> getModelListenerForException(Class<?> exceptionClass, String expectedMessage) {
        return ActionListener.<Model>wrap((model) -> fail("Model parsing should have failed"), e -> {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            assertThat(e.getMessage(), is(expectedMessage));
        });
    }

    public static void assertJsonEquals(String actual, String expected) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        try (
            var actualParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, actual);
            var expectedParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, expected);
        ) {
            assertThat(actualParser.map().entrySet(), containsInAnyOrder(expectedParser.map().entrySet().toArray()));
        }
    }

    public static <K, V> Map<K, V> modifiableMap(Map<K, V> aMap) {
        return new HashMap<>(aMap);
    }
}
