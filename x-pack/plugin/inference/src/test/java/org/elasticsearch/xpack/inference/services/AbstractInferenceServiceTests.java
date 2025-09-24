/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
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
public abstract class AbstractInferenceServiceTests extends InferenceServiceTestCase {

    private final TestConfiguration testConfiguration;

    protected final MockWebServer webServer = new MockWebServer();
    protected ThreadPool threadPool;
    protected HttpClientManager clientManager;
    protected TestCase testCase;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
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

    public AbstractInferenceServiceTests(TestConfiguration testConfiguration, TestCase testCase) {
        this.testConfiguration = Objects.requireNonNull(testConfiguration);
        this.testCase = testCase;
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
     * Configurations that are useful for most tests
     */
    public abstract static class CommonConfig {

        private final TaskType taskType;
        private final TaskType unsupportedTaskType;

        public CommonConfig(TaskType taskType, @Nullable TaskType unsupportedTaskType) {
            this.taskType = Objects.requireNonNull(taskType);
            this.unsupportedTaskType = unsupportedTaskType;
        }

        public TaskType taskType() {
            return taskType;
        }

        public TaskType unsupportedTaskType() {
            return unsupportedTaskType;
        }

        protected abstract SenderService createService(ThreadPool threadPool, HttpClientManager clientManager);

        protected abstract Map<String, Object> createServiceSettingsMap(TaskType taskType);

        protected Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
            return createServiceSettingsMap(taskType);
        }

        protected abstract Map<String, Object> createTaskSettingsMap();

        protected abstract Map<String, Object> createSecretSettingsMap();

        protected abstract void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets);

        protected void assertModel(Model model, TaskType taskType) {
            assertModel(model, taskType, true);
        }

        protected abstract EnumSet<TaskType> supportedStreamingTasks();
    }

    /**
     * Configurations specific to the {@link SenderService#updateModelWithEmbeddingDetails(Model, int)} tests
     */
    public abstract static class UpdateModelConfiguration {

        public boolean isEnabled() {
            return true;
        }

        protected abstract Model createEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure);
    }

    private static final UpdateModelConfiguration DISABLED_UPDATE_MODEL_TESTS = new UpdateModelConfiguration() {
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        protected Model createEmbeddingModel(SimilarityMeasure similarityMeasure) {
            throw new UnsupportedOperationException("Update model tests are disabled");
        }
    };

    @Override
    public InferenceService createInferenceService() {
        return testConfiguration.commonConfig.createService(threadPool, clientManager);
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModel() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, listener);

            var model = listener.actionGet(TIMEOUT);
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(Map.of());
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            parseRequestConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var chunkingSettingsMap = createRandomChunkingSettingsMap();
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                chunkingSettingsMap,
                parseRequestConfigTestConfig.createSecretSettingsMap()
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", TaskType.TEXT_EMBEDDING, config, listener);

            var model = listener.actionGet(TIMEOUT);
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            parseRequestConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testParseRequestConfig_CreatesACompletionModel() throws Exception {
        var parseRequestConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseRequestConfigTestConfig.createService(threadPool, clientManager)) {
            var config = getRequestConfigMap(
                parseRequestConfigTestConfig.createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.REQUEST),
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
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.taskType,
                    ConfigurationParseContext.REQUEST
                ),
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
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.taskType,
                    ConfigurationParseContext.REQUEST
                ),
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
            var serviceSettings = parseRequestConfigTestConfig.createServiceSettingsMap(
                parseRequestConfigTestConfig.taskType,
                ConfigurationParseContext.REQUEST
            );
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
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.taskType,
                    ConfigurationParseContext.REQUEST
                ),
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
                parseRequestConfigTestConfig.createServiceSettingsMap(
                    parseRequestConfigTestConfig.taskType,
                    ConfigurationParseContext.REQUEST
                ),
                parseRequestConfigTestConfig.createTaskSettingsMap(),
                secretSettingsMap
            );

            var listener = new PlainActionFuture<Model>();
            service.parseRequestConfig("id", parseRequestConfigTestConfig.taskType, config, listener);

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("Configuration contains settings [{extra_key=value}]"));
        }
    }

    @ParametersFactory
    public static Iterable<TestCase[]> parameters() throws IOException {
        var chunkingSettingsMap = createRandomChunkingSettingsMap();

        return Arrays.asList(
            new TestCase[][] {
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config without chunking settings",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig.createServiceSettingsMap(
                                TaskType.TEXT_EMBEDDING,
                                ConfigurationParseContext.PERSISTENT
                            ),
                            testConfiguration.commonConfig.createTaskSettingsMap(),
                            null
                        ),
                        (service, persistedConfig) -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config())
                    ).withNullChunkingSettingsMap().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with chunking settings",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig.createServiceSettingsMap(
                                TaskType.TEXT_EMBEDDING,
                                ConfigurationParseContext.PERSISTENT
                            ),
                            testConfiguration.commonConfig.createTaskSettingsMap(),
                            chunkingSettingsMap,
                            null
                        ),
                        (service, persistedConfig) -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfig.config())
                    ).withChunkingSettingsMap(chunkingSettingsMap).build() } }
        );
    }

    public record TestCase(
        String description,
        Function<TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
        BiFunction<SenderService, Utils.PersistedConfig, Model> serviceCallback,
        @Nullable Map<String, Object> chunkingSettingsMap,
        boolean validateChunkingSettings,
        boolean modelIncludesSecrets
    ) {}

    private static class TestCaseBuilder {
        private final String description;
        private final Function<TestConfiguration, Utils.PersistedConfig> createPersistedConfig;
        private final BiFunction<SenderService, Utils.PersistedConfig, Model> serviceCallback;
        @Nullable
        private Map<String, Object> chunkingSettingsMap;
        private boolean validateChunkingSettings;
        private boolean modelIncludesSecrets;

        TestCaseBuilder(
            String description,
            Function<TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
            BiFunction<SenderService, Utils.PersistedConfig, Model> serviceCallback
        ) {
            this.description = description;
            this.createPersistedConfig = createPersistedConfig;
            this.serviceCallback = serviceCallback;
        }

        public TestCaseBuilder withSecrets() {
            this.modelIncludesSecrets = true;
            return this;
        }

        public TestCaseBuilder withChunkingSettingsMap(Map<String, Object> chunkingSettingsMap) {
            this.chunkingSettingsMap = chunkingSettingsMap;
            this.validateChunkingSettings = true;
            return this;
        }

        /**
         * Use an empty chunking settings map but still do validation that the chunking settings are set to the appropriate
         * defaults.
         */
        public TestCaseBuilder withEmptyChunkingSettingsMap() {
            this.chunkingSettingsMap = Map.of();
            this.validateChunkingSettings = true;
            return this;
        }

        public TestCaseBuilder withNullChunkingSettingsMap() {
            this.chunkingSettingsMap = null;
            this.validateChunkingSettings = true;
            return this;
        }

        public TestCase build() {
            return new TestCase(
                description,
                createPersistedConfig,
                serviceCallback,
                chunkingSettingsMap,
                validateChunkingSettings,
                modelIncludesSecrets
            );
        }
    }

    public void testPersistedConfig() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        var persistedConfig = testCase.createPersistedConfig.apply(testConfiguration);

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var model = testCase.serviceCallback.apply(service, persistedConfig);

            if (testCase.validateChunkingSettings) {
                var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(testCase.chunkingSettingsMap);
                assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            }

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    // parsePersistedConfig tests

    public void testParsePersistedConfig_CreatesAnEmbeddingsModel() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        var persistedConfigMap = getPersistedConfigMap(
            parseConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
            parseConfigTestConfig.createTaskSettingsMap(),
            null
        );

        parseConfigHelper(service -> service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, persistedConfigMap.config()), null);
    }

    private void parseConfigHelper(Function<SenderService, Model> serviceParseCallback, @Nullable Map<String, Object> chunkingSettingsMap)
        throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {

            var model = serviceParseCallback.apply(service);

            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap == null ? Map.of() : chunkingSettingsMap);
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));

            parseConfigTestConfig.assertModel(model, TaskType.TEXT_EMBEDDING);
        }
    }

    // parsePersistedConfigWithSecrets

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModel() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        var persistedConfigMap = getPersistedConfigMap(
            parseConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
            parseConfigTestConfig.createTaskSettingsMap(),
            parseConfigTestConfig.createSecretSettingsMap()
        );

        parseConfigHelper(
            service -> service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            ),
            null
        );
    }

    public void testParsePersistedConfigWithSecrets_CreatesAnEmbeddingsModelWhenChunkingSettingsAreProvided() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        var chunkingSettingsMap = createRandomChunkingSettingsMap();
        var persistedConfigMap = getPersistedConfigMap(
            parseConfigTestConfig.createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
            parseConfigTestConfig.createTaskSettingsMap(),
            chunkingSettingsMap,
            parseConfigTestConfig.createSecretSettingsMap()
        );

        parseConfigHelper(
            service -> service.parsePersistedConfigWithSecrets(
                "id",
                TaskType.TEXT_EMBEDDING,
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            ),
            chunkingSettingsMap
        );
    }

    public void testParsePersistedConfigWithSecrets_CreatesACompletionModel() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
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
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType, ConfigurationParseContext.PERSISTENT),
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
                containsString(
                    Strings.format(fetchPersistedConfigTaskTypeParsingErrorMessageFormat(), parseConfigTestConfig.unsupportedTaskType)
                )
            );
        }
    }

    protected String fetchPersistedConfigTaskTypeParsingErrorMessageFormat() {
        return "service does not support task type [%s]";
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType, ConfigurationParseContext.PERSISTENT),
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

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var serviceSettings = parseConfigTestConfig.createServiceSettingsMap(
                parseConfigTestConfig.taskType,
                ConfigurationParseContext.PERSISTENT
            );
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

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var taskSettings = parseConfigTestConfig.createTaskSettingsMap();
            taskSettings.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType, ConfigurationParseContext.PERSISTENT),
                taskSettings,
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType, config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType);
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig;
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var secretSettingsMap = parseConfigTestConfig.createSecretSettingsMap();
            secretSettingsMap.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType, ConfigurationParseContext.PERSISTENT),
                parseConfigTestConfig.createTaskSettingsMap(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType, config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType);
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

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration.isEnabled());

        try (var service = testConfiguration.commonConfig.createService(threadPool, clientManager)) {
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.updateModelWithEmbeddingDetails(getInvalidModel("id", "service"), randomNonNegativeInt())
            );

            assertThat(exception.getMessage(), containsString("Can't update embedding details for model"));
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
