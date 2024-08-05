/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpSettings;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettings;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class Utils {

    private Utils() {
        throw new UnsupportedOperationException("Utils is a utility class and should not be instantiated");
    }

    public static ClusterService mockClusterServiceEmpty() {
        return mockClusterService(Settings.EMPTY);
    }

    public static ClusterService mockClusterService(Settings settings) {
        var clusterService = mock(ClusterService.class);

        var registeredSettings = Stream.of(
            HttpSettings.getSettingsDefinitions(),
            HttpClientManager.getSettingsDefinitions(),
            ThrottlerManager.getSettingsDefinitions(),
            RetrySettings.getSettingsDefinitions(),
            Truncator.getSettingsDefinitions(),
            RequestExecutorServiceSettings.getSettingsDefinitions()
        ).flatMap(Collection::stream).collect(Collectors.toSet());

        var cSettings = new ClusterSettings(settings, registeredSettings);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);

        return clusterService;
    }

    public static ScalingExecutorBuilder inferenceUtilityPool() {
        return new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            1,
            4,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.inference.utility_thread_pool"
        );
    }

    public static void storeSparseModel(Client client) throws Exception {
        Model model = new TestSparseInferenceServiceExtension.TestSparseModel(
            TestSparseInferenceServiceExtension.TestInferenceService.NAME,
            new TestSparseInferenceServiceExtension.TestServiceSettings("sparse_model", null, false)
        );
        storeModel(client, model);
    }

    public static void storeDenseModel(
        Client client,
        int dimensions,
        SimilarityMeasure similarityMeasure,
        DenseVectorFieldMapper.ElementType elementType
    ) throws Exception {
        Model model = new TestDenseInferenceServiceExtension.TestDenseModel(
            TestDenseInferenceServiceExtension.TestInferenceService.NAME,
            new TestDenseInferenceServiceExtension.TestServiceSettings("dense_model", dimensions, similarityMeasure, elementType)
        );

        storeModel(client, model);
    }

    public static void storeModel(Client client, Model model) throws Exception {
        ModelRegistry modelRegistry = new ModelRegistry(client);

        AtomicReference<Boolean> storeModelHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> modelRegistry.storeModel(model, listener), storeModelHolder, exceptionHolder);

        assertThat(storeModelHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    private static <T> void blockingCall(
        Consumer<ActionListener<T>> function,
        AtomicReference<T> response,
        AtomicReference<Exception> error
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }

    public static class TestInferencePlugin extends InferencePlugin {
        public TestInferencePlugin(Settings settings) {
            super(settings);
        }

        @Override
        public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
            return List.of(
                TestSparseInferenceServiceExtension.TestInferenceService::new,
                TestDenseInferenceServiceExtension.TestInferenceService::new
            );
        }
    }

    public static Model getInvalidModel(String inferenceEntityId, String serviceName) {
        var mockConfigs = mock(ModelConfigurations.class);
        when(mockConfigs.getInferenceEntityId()).thenReturn(inferenceEntityId);
        when(mockConfigs.getService()).thenReturn(serviceName);

        var mockModel = mock(Model.class);
        when(mockModel.getConfigurations()).thenReturn(mockConfigs);

        return mockModel;
    }

    public static SimilarityMeasure randomSimilarityMeasure() {
        return randomFrom(SimilarityMeasure.values());
    }

    public record PersistedConfig(Map<String, Object> config, Map<String, Object> secrets) {}

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
}
