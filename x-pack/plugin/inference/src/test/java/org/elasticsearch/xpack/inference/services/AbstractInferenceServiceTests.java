/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.custom.CustomModel;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * Base class for testing inference services.
 * <p>
 * This class provides common unit tests for inference services, such as testing the model creation, and calling the infer method.
 *
 * To use this class, extend it and pass the constructor a configuration.
 * </p>
 */
public abstract class AbstractInferenceServiceTests extends ESTestCase {

    protected final MockWebServer webServer = new MockWebServer();
    protected ThreadPool threadPool;
    protected HttpClientManager clientManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    private final TestConfiguration testConfiguration;

    public AbstractInferenceServiceTests(TestConfiguration testConfiguration) {
        this.testConfiguration = Objects.requireNonNull(testConfiguration);
    }

    /**
     * Main configurations for the tests
     */
    public record TestConfiguration(CommonConfig commonConfig, UpdateModelConfiguration updateModelConfiguration) {
        public static class Builder {
            private final CommonConfig commonConfig;
            private UpdateModelConfiguration updateModelConfiguration = DISABLED_UPDATE_MODEL_TESTS;

            public Builder(CommonConfig commonConfig) {
                this.commonConfig = commonConfig;
            }

            public Builder enableUpdateModelTests(UpdateModelConfiguration updateModelConfiguration) {
                this.updateModelConfiguration = updateModelConfiguration;
                return this;
            }

            public TestConfiguration build() {
                return new TestConfiguration(commonConfig, updateModelConfiguration);
            }
        }
    }

    /**
     * Configurations that useful for most tests
     */
    public abstract static class CommonConfig {

        private final TaskType taskType;
        private final TaskType unsupportedTaskType;

        public CommonConfig(TaskType taskType, @Nullable TaskType unsupportedTaskType) {
            this.taskType = Objects.requireNonNull(taskType);
            this.unsupportedTaskType = unsupportedTaskType;
        }

        protected abstract SenderService createService(ThreadPool threadPool, HttpClientManager clientManager);

        protected abstract Map<String, Object> createServiceSettingsMap(TaskType taskType);

        protected abstract Map<String, Object> createTaskSettingsMap();

        protected abstract Map<String, Object> createSecretSettingsMap();

        protected abstract void assertModel(Model model, TaskType taskType);

        protected abstract EnumSet<TaskType> supportedStreamingTasks();
    }

    /**
     * Configurations specific to the {@link SenderService#updateModelWithEmbeddingDetails(Model, int)} tests
     */
    public abstract static class UpdateModelConfiguration {

        public boolean isEnabled() {
            return true;
        }

        protected abstract CustomModel createEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure);
    }

    private static final UpdateModelConfiguration DISABLED_UPDATE_MODEL_TESTS = new UpdateModelConfiguration() {
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        protected CustomModel createEmbeddingModel(SimilarityMeasure similarityMeasure) {
            throw new UnsupportedOperationException("Update model tests are disabled");
        }
    };

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, listener);

            parseRequestConfigTestConfig.assertModel(listener.actionGet(TIMEOUT), TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParseRequestConfig_CreatesACompletionModel() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.COMPLETION),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.COMPLETION, config, listener);

            parseRequestConfigTestConfig.assertModel(listener.actionGet(TIMEOUT), TaskType.COMPLETION);
        }
    }

    public void testParseRequestConfig_ThrowsUnsupportedModelType() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(parseRequestConfigTestConfig.taskType),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.unsupportedTaskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                containsString(Strings.format("service does not support task type [%s]", parseRequestConfigTestConfig.unsupportedTaskType))
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInConfig() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(parseRequestConfigTestConfig.taskType),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );
            config.put("extra_key", "value");

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.taskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var serviceSettings = parseRequestConfigTestConfig.createServiceSettingsMap(parseRequestConfigTestConfig.taskType);
            serviceSettings.put("extra_key", "value");
            var config = getRequestConfigMap(
                serviceSettings,
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.taskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var taskSettings = parseRequestConfigTestConfig.createTaskSettingsMap();
            taskSettings.put("extra_key", "value");
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(parseRequestConfigTestConfig.taskType),
                taskSettings,
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.taskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var secretSettingsMap = parseRequestConfigTestConfig.createSecretSettingsMap();
            secretSettingsMap.put("extra_key", "value");
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(parseRequestConfigTestConfig.taskType),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                secretSettingsMap
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.taskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    // parsePersistedConfigWithSecrets

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModel() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );
            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParsePersistedConfigWithSecrets_CreatesACompletionModel() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(TaskType.COMPLETION),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.COMPLETION,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );
            parseConfigTestConfig.assertModel(model, TaskType.COMPLETION);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsUnsupportedModelType() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets(
                    "id",
                    parseConfigTestConfig.unsupportedTaskType,
                    persistedConfigMap.config(),
                    persistedConfigMap.secrets()
                )
            );

            assertThat(
                exception.getMessage(),
                containsString(Strings.format("service does not support task type [%s]", parseConfigTestConfig.unsupportedTaskType))
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );
            persistedConfigMap.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                parseConfigTestConfig.taskType,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var serviceSettings = parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType);
            serviceSettings.put("extra_key", "value");
            var persistedConfigMap = getPersistedConfigMap(
                serviceSettings,
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                parseConfigTestConfig.taskType,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var taskSettings = parseConfigTestConfig.createTaskSettingsMap();
            taskSettings.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType),
                taskSettings,
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType, config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var secretSettingsMap = parseConfigTestConfig.createSecretSettingsMap();
            secretSettingsMap.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType),
                parseConfigTestConfig.createTaskSettingsMap(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType, config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotValid() throws IOException {
        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var listener = new PlainActionFuture<InferenceServiceResults>();

            service.infer(
                getInvalidModel("id", "service"),
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.INTERNAL_SEARCH,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                is("The internal model was invalid, please delete the service [service] with id [id] and add it again.")
            );
        }
    }

    public void testInfer_ThrowsErrorWhenInputTypeIsSpecified() throws IOException {
        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var listener = new PlainActionFuture<InferenceServiceResults>();

            var exception = expectThrows(
                ValidationException.class,
                () -> service.infer(
                    getInvalidModel("id", "service"),
                    null,
                    null,
                    null,
                    List.of(""),
                    false,
                    new HashMap<>(),
                    InputType.INGEST,
                    InferenceAction.Request.DEFAULT_TIMEOUT,
                    listener
                )
            );

            assertThat(
                exception.getMessage(),
                is("Validation Failed: 1: Invalid input_type [ingest]. The input_type option is not supported by this service;")
            );
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration.isEnabled());

        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.updateModelWithEmbeddingDetails(getInvalidModel("id", "service"), randomNonNegativeInt())
            );

            assertThat(exception.getMessage(), containsString("Can't update embedding details for model of type:"));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration.isEnabled());

        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration.createEmbeddingModel(null);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.DOT_PRODUCT, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration.isEnabled());

        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration.createEmbeddingModel(SimilarityMeasure.COSINE);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.COSINE, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    // streaming tests
    public void testSupportedStreamingTasks() throws Exception {
        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            assertThat(service.supportedStreamingTasks(), is(testConfiguration.commonConfig.supportedStreamingTasks()));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }
}
