/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.Utils;
import org.junit.Assume;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Base class for testing inference services' parsing of persisted configurations using parameterized tests.
 */
public abstract class AbstractInferenceServiceParameterizedParsingTests extends AbstractInferenceServiceParameterizedTests {

    private final AbstractInferenceServiceParameterizedParsingTests.TestCase testCase;

    public AbstractInferenceServiceParameterizedParsingTests(
        AbstractInferenceServiceBaseTests.TestConfiguration testConfiguration,
        TestCase testCase
    ) {
        super(testConfiguration);
        this.testCase = testCase;
    }

    @Override
    public InferenceService createInferenceService() {
        return testConfiguration.commonConfig().createService(threadPool, clientManager);
    }

    public record TestCase(
        String description,
        Function<AbstractInferenceServiceBaseTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
        ServiceParser serviceParser,
        TaskType expectedTaskType,
        boolean modelIncludesSecrets,
        boolean expectFailure
    ) {}

    private record ServiceParserParams(
        SenderService service,
        Utils.PersistedConfig persistedConfig,
        AbstractInferenceServiceBaseTests.TestConfiguration testConfiguration
    ) {}

    @FunctionalInterface
    private interface ServiceParser {
        Model parseConfigs(ServiceParserParams params);
    }

    private static class TestCaseBuilder {
        private final String description;
        private final Function<AbstractInferenceServiceBaseTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig;
        private final ServiceParser serviceParser;
        private final TaskType expectedTaskType;
        private boolean modelIncludesSecrets;
        private boolean expectFailure;

        TestCaseBuilder(
            String description,
            Function<AbstractInferenceServiceBaseTests.TestConfiguration, Utils.PersistedConfig> createPersistedConfig,
            ServiceParser serviceParser,
            TaskType expectedTaskType
        ) {
            this.description = description;
            this.createPersistedConfig = createPersistedConfig;
            this.serviceParser = serviceParser;
            this.expectedTaskType = expectedTaskType;
        }

        public TestCaseBuilder withSecrets() {
            this.modelIncludesSecrets = true;
            return this;
        }

        public TestCaseBuilder expectFailure() {
            this.expectFailure = true;
            return this;
        }

        public TestCase build() {
            return new TestCase(description, createPersistedConfig, serviceParser, expectedTaskType, modelIncludesSecrets, expectFailure);
        }
    }

    @ParametersFactory
    public static Iterable<TestCase[]> parameters() {
        return Arrays.asList(
            new TestCase[][] {
                // Test cases for parsePersistedConfig method
                {
                    new TestCaseBuilder(
                        "Test parsing text embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, false, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing text embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, true, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, false, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, true, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, false, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, true, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing completion persisted config returns model",
                        persistedConfig(TaskType.COMPLETION, false, false),
                        getServiceParser(TaskType.COMPLETION),
                        TaskType.COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing chat completion persisted config returns model",
                        persistedConfig(TaskType.CHAT_COMPLETION, false, false),
                        getServiceParser(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing rerank persisted config returns model",
                        persistedConfig(TaskType.RERANK, false, false),
                        getServiceParser(TaskType.RERANK),
                        TaskType.RERANK
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config throws exception for unsupported task type",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(
                                    testConfiguration.commonConfig().targetTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                ),
                            testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                            null
                        ),
                        params -> params.service.parsePersistedConfig(
                            "id",
                            params.testConfiguration.commonConfig().unsupportedTaskType(),
                            params.persistedConfig.config()
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null
                    ).expectFailure().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in config",
                        testConfiguration -> {
                            var persistedConfigMap = getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(
                                        testConfiguration.commonConfig().targetTaskType(),
                                        ConfigurationParseContext.PERSISTENT
                                    ),
                                testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                                null
                            );
                            persistedConfigMap.config().put("extra_key", "value");
                            return persistedConfigMap;
                        },
                        params -> params.service.parsePersistedConfig(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in service settings",
                        testConfiguration -> {
                            var serviceSettings = testConfiguration.commonConfig()
                                .createServiceSettingsMap(
                                    testConfiguration.commonConfig().targetTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                );
                            serviceSettings.put("extra_key", "value");

                            return getPersistedConfigMap(
                                serviceSettings,
                                testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                                null
                            );
                        },
                        params -> params.service.parsePersistedConfig(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in task settings",
                        testConfiguration -> {
                            var taskSettingsMap = testConfiguration.commonConfig()
                                .createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType());
                            taskSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(
                                        testConfiguration.commonConfig().targetTaskType(),
                                        ConfigurationParseContext.PERSISTENT
                                    ),
                                taskSettingsMap,
                                null
                            );
                        },
                        params -> params.service.parsePersistedConfig(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).build() },
                // Test cases for parsePersistedConfigWithSecrets method
                {
                    new TestCaseBuilder(
                        "Test parsing text embeddings persisted config with secrets, without chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, false, true),
                        getServiceParserWithSecrets(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing text embeddings persisted config with secrets, with chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, true, true),
                        getServiceParserWithSecrets(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embeddings persisted config with secrets, without chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, false, true),
                        getServiceParserWithSecrets(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embeddings persisted config with secrets, with chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, true, true),
                        getServiceParserWithSecrets(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embeddings persisted config with secrets, without chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, false, true),
                        getServiceParserWithSecrets(TaskType.EMBEDDING),
                        TaskType.EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embeddings persisted config with secrets, with chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, true, true),
                        getServiceParserWithSecrets(TaskType.EMBEDDING),
                        TaskType.EMBEDDING
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing completion persisted config with secrets returns model",
                        persistedConfig(TaskType.COMPLETION, false, true),
                        getServiceParserWithSecrets(TaskType.COMPLETION),
                        TaskType.COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing chat completion persisted config with secrets returns model",
                        persistedConfig(TaskType.CHAT_COMPLETION, false, true),
                        getServiceParserWithSecrets(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing rerank persisted config with secrets returns model",
                        persistedConfig(TaskType.RERANK, false, true),
                        getServiceParserWithSecrets(TaskType.RERANK),
                        TaskType.RERANK
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets throws exception for unsupported task type",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(
                                    testConfiguration.commonConfig().targetTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                ),
                            testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        params -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().unsupportedTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null
                    ).withSecrets().expectFailure().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets does not throw when an extra key exists in config",
                        testConfiguration -> {
                            var persistedConfigMap = getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(
                                        testConfiguration.commonConfig().targetTaskType(),
                                        ConfigurationParseContext.PERSISTENT
                                    ),
                                testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                            persistedConfigMap.config().put("extra_key", "value");
                            return persistedConfigMap;
                        },
                        params -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        null
                        // Test expected task type is the target task type
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets does not throw when an extra key exists in service settings",
                        testConfiguration -> {
                            var serviceSettings = testConfiguration.commonConfig()
                                .createServiceSettingsMap(
                                    testConfiguration.commonConfig().targetTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                );
                            serviceSettings.put("extra_key", "value");

                            return getPersistedConfigMap(
                                serviceSettings,
                                testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                        },
                        params -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets does not throw when an extra key exists in task settings",
                        testConfiguration -> {
                            var taskSettingsMap = testConfiguration.commonConfig()
                                .createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType());
                            taskSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(
                                        testConfiguration.commonConfig().targetTaskType(),
                                        ConfigurationParseContext.PERSISTENT
                                    ),
                                taskSettingsMap,
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                        },
                        params -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets does not throw when an extra key exists in secret settings",
                        testConfiguration -> {
                            var secretSettingsMap = testConfiguration.commonConfig().createSecretSettingsMap();
                            secretSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(
                                        testConfiguration.commonConfig().targetTaskType(),
                                        ConfigurationParseContext.PERSISTENT
                                    ),
                                testConfiguration.commonConfig().createTaskSettingsMap(testConfiguration.commonConfig().targetTaskType()),
                                secretSettingsMap
                            );
                        },
                        params -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().targetTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        // Test expected task type is the target task type
                        null
                    ).withSecrets().build() } }
        );
    }

    private static Function<TestConfiguration, Utils.PersistedConfig> persistedConfig(
        TaskType taskType,
        boolean withChunking,
        boolean withSecrets
    ) {
        return testConfiguration -> getPersistedConfigMap(
            testConfiguration.commonConfig().createServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
            testConfiguration.commonConfig().createTaskSettingsMap(taskType),
            withChunking ? createRandomChunkingSettingsMap() : null,
            withSecrets ? testConfiguration.commonConfig().createSecretSettingsMap() : null
        );
    }

    private static ServiceParser getServiceParser(TaskType taskType) {
        return params -> params.service.parsePersistedConfig("id", taskType, params.persistedConfig.config());
    }

    private static ServiceParser getServiceParserWithSecrets(TaskType taskType) {
        return params -> params.service.parsePersistedConfigWithSecrets(
            "id",
            taskType,
            params.persistedConfig.config(),
            params.persistedConfig.secrets()
        );
    }

    public void testPersistedConfig() throws Exception {
        CommonConfig commonConfig = testConfiguration.commonConfig();
        // If the service doesn't support the expected task type, then skip the test
        Assume.assumeTrue(
            "Service doesn't support task type",
            testCase.expectedTaskType == null || commonConfig.supportedTaskTypes().contains(testCase.expectedTaskType)
        );

        var persistedConfig = testCase.createPersistedConfig.apply(testConfiguration);
        try (var service = commonConfig.createService(threadPool, clientManager)) {

            if (testCase.expectFailure) {
                assertFailedParse(service, persistedConfig);
            } else {
                assertSuccessfulParse(service, persistedConfig);
            }
        }
    }

    private void assertFailedParse(SenderService service, Utils.PersistedConfig persistedConfig) {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> testCase.serviceParser.parseConfigs(new ServiceParserParams(service, persistedConfig, testConfiguration))
        );

        assertThat(
            exception.getMessage(),
            containsString(
                Strings.format("service does not support task type [%s]", testConfiguration.commonConfig().unsupportedTaskType())
            )
        );
    }

    private void assertSuccessfulParse(SenderService service, Utils.PersistedConfig persistedConfig) {
        var model = testCase.serviceParser.parseConfigs(new ServiceParserParams(service, persistedConfig, testConfiguration));

        if (persistedConfig.config().containsKey(ModelConfigurations.CHUNKING_SETTINGS)) {
            @SuppressWarnings("unchecked")
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(
                (Map<String, Object>) persistedConfig.config().get(ModelConfigurations.CHUNKING_SETTINGS)
            );
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
        }

        testConfiguration.commonConfig()
            .assertModel(
                model,
                // Use the expected task type from the test case if provided, otherwise use the task type from the model configurations
                testCase.expectedTaskType != null ? testCase.expectedTaskType : model.getConfigurations().getTaskType(),
                testCase.modelIncludesSecrets
            );
    }
}
