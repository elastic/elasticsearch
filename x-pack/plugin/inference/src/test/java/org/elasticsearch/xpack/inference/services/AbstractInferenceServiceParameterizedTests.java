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
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * Base class for testing inference services using parameterized tests.
 */
public abstract class AbstractInferenceServiceParameterizedTests extends InferenceServiceTestCase {

    private final AbstractInferenceServiceTests.TestConfiguration testConfiguration;

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

    public AbstractInferenceServiceParameterizedTests(
        AbstractInferenceServiceTests.TestConfiguration testConfiguration,
        TestCase testCase
    ) {
        this.testConfiguration = Objects.requireNonNull(testConfiguration);
        this.testCase = testCase;
    }

    @Override
    public InferenceService createInferenceService() {
        return testConfiguration.commonConfig().createService(threadPool, clientManager);
    }

    @ParametersFactory
    public static Iterable<TestCase[]> parameters() throws IOException {
        return Arrays.asList(
            new TestCase[][] {
                // parsePersistedConfig
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config without chunking settings",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            null
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfig(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            persistedConfig.config()
                        ),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with chunking settings",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            createRandomChunkingSettingsMap(),
                            null
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfig(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            persistedConfig.config()
                        ),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                // parsePersistedConfigWithSecrets
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets creates an embeddings model",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            persistedConfig.config(),
                            persistedConfig.secrets()
                        ),
                        TaskType.TEXT_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets creates an embeddings "
                            + "model when chunking settings are provided",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            createRandomChunkingSettingsMap(),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            persistedConfig.config(),
                            persistedConfig.secrets()
                        ),
                        TaskType.TEXT_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets creates a completion "
                            + "model when chunking settings are not provided",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            persistedConfig.config(),
                            persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets throws exception for unsupported task type",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        (service, persistedConfig, testConfiguration) -> service.parsePersistedConfigWithSecrets(
                            "id",
                            testConfiguration.commonConfig().unsupportedTaskType(),
                            persistedConfig.config(),
                            persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() } }
        );
    }

    public record TestCase(
        String description,
        Function<AbstractInferenceServiceTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
        ServiceCallback serviceCallback,
        TaskType expectedTaskType,
        boolean modelIncludesSecrets,
        boolean expectFailure
    ) {}

    @FunctionalInterface
    interface ServiceCallback {
        Model parseConfigs(
            SenderService service,
            Utils.PersistedConfig persistedConfig,
            AbstractInferenceServiceTests.TestConfiguration testConfiguration
        );
    }

    private static class TestCaseBuilder {
        private final String description;
        private final Function<AbstractInferenceServiceTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig;
        private final ServiceCallback serviceCallback;
        private final TaskType expectedTaskType;
        private boolean modelIncludesSecrets;
        private boolean expectFailure;

        TestCaseBuilder(
            String description,
            Function<AbstractInferenceServiceTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
            ServiceCallback serviceCallback,
            TaskType expectedTaskType
        ) {
            this.description = description;
            this.createPersistedConfig = createPersistedConfig;
            this.serviceCallback = serviceCallback;
            this.expectedTaskType = expectedTaskType;
        }

        public TestCaseBuilder withSecrets() {
            this.modelIncludesSecrets = true;
            return this;
        }

        public TestCaseBuilder withFailure() {
            this.expectFailure = true;
            return this;
        }

        public TestCase build() {
            return new TestCase(description, createPersistedConfig, serviceCallback, expectedTaskType, modelIncludesSecrets, expectFailure);
        }
    }

    public void testPersistedConfig() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig();
        var persistedConfig = testCase.createPersistedConfig.apply(testConfiguration);

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var model = testCase.serviceCallback.parseConfigs(service, persistedConfig, testConfiguration);

            if (persistedConfig.config().containsKey(ModelConfigurations.CHUNKING_SETTINGS)) {
                @SuppressWarnings("unchecked")
                var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(
                    (Map<String, Object>) persistedConfig.config().get(ModelConfigurations.CHUNKING_SETTINGS)
                );
                assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            }

            parseConfigTestConfig.assertModel(model, testCase.expectedTaskType, testCase.modelIncludesSecrets);
        }
    }

    // parsePersistedConfigWithSecrets

    public void testParsePersistedConfigWithSecrets_ThrowsUnsupportedModelType() throws Exception {
        var parseConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType(), ConfigurationParseContext.PERSISTENT),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.parsePersistedConfigWithSecrets(
                    "id",
                    parseConfigTestConfig.unsupportedTaskType(),
                    persistedConfigMap.config(),
                    persistedConfigMap.secrets()
                )
            );

            assertThat(
                exception.getMessage(),
                containsString(
                    Strings.format(fetchPersistedConfigTaskTypeParsingErrorMessageFormat(), parseConfigTestConfig.unsupportedTaskType())
                )
            );
        }
    }

    protected String fetchPersistedConfigTaskTypeParsingErrorMessageFormat() {
        return "service does not support task type [%s]";
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInConfig() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig();

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var persistedConfigMap = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType(), ConfigurationParseContext.PERSISTENT),
                parseConfigTestConfig.createTaskSettingsMap(),
                parseConfigTestConfig.createSecretSettingsMap()
            );
            persistedConfigMap.config().put("extra_key", "value");

            var model = service.parsePersistedConfigWithSecrets(
                "id",
                parseConfigTestConfig.taskType(),
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType());
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInServiceSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var serviceSettings = parseConfigTestConfig.createServiceSettingsMap(
                parseConfigTestConfig.taskType(),
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
                parseConfigTestConfig.taskType(),
                persistedConfigMap.config(),
                persistedConfigMap.secrets()
            );

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType());
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInTaskSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var taskSettings = parseConfigTestConfig.createTaskSettingsMap();
            taskSettings.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType(), ConfigurationParseContext.PERSISTENT),
                taskSettings,
                parseConfigTestConfig.createSecretSettingsMap()
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType(), config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType());
        }
    }

    public void testParsePersistedConfigWithSecrets_ThrowsWhenAnExtraKeyExistsInSecretSettingsMap() throws IOException {
        var parseConfigTestConfig = testConfiguration.commonConfig();
        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {
            var secretSettingsMap = parseConfigTestConfig.createSecretSettingsMap();
            secretSettingsMap.put("extra_key", "value");
            var config = getPersistedConfigMap(
                parseConfigTestConfig.createServiceSettingsMap(parseConfigTestConfig.taskType(), ConfigurationParseContext.PERSISTENT),
                parseConfigTestConfig.createTaskSettingsMap(),
                secretSettingsMap
            );

            var model = service.parsePersistedConfigWithSecrets("id", parseConfigTestConfig.taskType(), config.config(), config.secrets());

            parseConfigTestConfig.assertModel(model, parseConfigTestConfig.taskType());
        }
    }

    public void testInfer_ThrowsErrorWhenModelIsNotValid() throws IOException {
        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
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
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> service.updateModelWithEmbeddingDetails(getInvalidModel("id", "service"), randomNonNegativeInt())
            );

            assertThat(exception.getMessage(), containsString("Can't update embedding details for model"));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration().createEmbeddingModel(null);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.DOT_PRODUCT, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel() throws IOException {
        Assume.assumeTrue(testConfiguration.updateModelConfiguration().isEnabled());

        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            var embeddingSize = randomNonNegativeInt();
            var model = testConfiguration.updateModelConfiguration().createEmbeddingModel(SimilarityMeasure.COSINE);

            Model updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(SimilarityMeasure.COSINE, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    // streaming tests
    public void testSupportedStreamingTasks() throws Exception {
        try (var service = testConfiguration.commonConfig().createService(threadPool, clientManager)) {
            assertThat(service.supportedStreamingTasks(), is(testConfiguration.commonConfig().supportedStreamingTasks()));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }
}
