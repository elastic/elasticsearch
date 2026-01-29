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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Base class for testing inference services using parameterized tests.
 */
public abstract class AbstractInferenceServiceParameterizedTests extends AbstractInferenceServiceBaseTests {

    public AbstractInferenceServiceParameterizedTests(
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
    public static Iterable<TestCase[]> parameters() throws IOException {
        return Arrays.asList(
            new TestCase[][] {
                // Test cases for parsePersistedConfig method
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config without chunking settings",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            null
                        ),
                        (params) -> params.service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, params.persistedConfig.config()),
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
                        (params) -> params.service.parsePersistedConfig("id", TaskType.TEXT_EMBEDDING, params.persistedConfig.config()),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in config",
                        testConfiguration -> {
                            var persistedConfigMap = getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                                testConfiguration.commonConfig().createTaskSettingsMap(),
                                null
                            );
                            persistedConfigMap.config().put("extra_key", "value");
                            return persistedConfigMap;
                        },
                        (params) -> params.service.parsePersistedConfig("id", TaskType.COMPLETION, params.persistedConfig.config()),
                        TaskType.COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in service settings",
                        testConfiguration -> {
                            var serviceSettings = testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT);
                            serviceSettings.put("extra_key", "value");

                            return getPersistedConfigMap(serviceSettings, testConfiguration.commonConfig().createTaskSettingsMap(), null);
                        },
                        (params) -> params.service.parsePersistedConfig("id", TaskType.COMPLETION, params.persistedConfig.config()),
                        TaskType.COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in task settings",
                        testConfiguration -> {
                            var taskSettingsMap = testConfiguration.commonConfig().createTaskSettingsMap();
                            taskSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                                taskSettingsMap,
                                null
                            );
                        },
                        (params) -> params.service.parsePersistedConfig("id", TaskType.COMPLETION, params.persistedConfig.config()),
                        TaskType.COMPLETION
                    ).build() },
                // Test cases for parsePersistedConfigWithSecrets method
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with secrets creates an embeddings model",
                        testConfiguration -> getPersistedConfigMap(
                            testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.TEXT_EMBEDDING, ConfigurationParseContext.PERSISTENT),
                            testConfiguration.commonConfig().createTaskSettingsMap(),
                            testConfiguration.commonConfig().createSecretSettingsMap()
                        ),
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
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
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.TEXT_EMBEDDING,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
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
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
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
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            params.testConfiguration.commonConfig().unsupportedTaskType(),
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().expectFailure().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets does not throw when an extra key exists in config",
                        testConfiguration -> {
                            var persistedConfigMap = getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                                testConfiguration.commonConfig().createTaskSettingsMap(),
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                            persistedConfigMap.config().put("extra_key", "value");
                            return persistedConfigMap;
                        },
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets does not throw when an extra key exists in service settings",
                        testConfiguration -> {
                            var serviceSettings = testConfiguration.commonConfig()
                                .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT);
                            serviceSettings.put("extra_key", "value");

                            return getPersistedConfigMap(
                                serviceSettings,
                                testConfiguration.commonConfig().createTaskSettingsMap(),
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                        },
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets does not throw when an extra key exists in task settings",
                        testConfiguration -> {
                            var taskSettingsMap = testConfiguration.commonConfig().createTaskSettingsMap();
                            taskSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                                taskSettingsMap,
                                testConfiguration.commonConfig().createSecretSettingsMap()
                            );
                        },
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config with with secrets does not throw when an extra key exists in secret settings",
                        testConfiguration -> {
                            var secretSettingsMap = testConfiguration.commonConfig().createSecretSettingsMap();
                            secretSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfiguration.commonConfig()
                                    .createServiceSettingsMap(TaskType.COMPLETION, ConfigurationParseContext.PERSISTENT),
                                testConfiguration.commonConfig().createTaskSettingsMap(),
                                secretSettingsMap
                            );
                        },
                        (params) -> params.service.parsePersistedConfigWithSecrets(
                            "id",
                            TaskType.COMPLETION,
                            params.persistedConfig.config(),
                            params.persistedConfig.secrets()
                        ),
                        TaskType.COMPLETION
                    ).withSecrets().build() } }
        );
    }

    public void testPersistedConfig() throws Exception {
        // If the service doesn't support the expected task type, then skip the test
        Assume.assumeTrue(testConfiguration.commonConfig().supportedTaskTypes().contains(testCase.expectedTaskType));

        var parseConfigTestConfig = testConfiguration.commonConfig();
        var persistedConfig = testCase.createPersistedConfig.apply(testConfiguration);

        try (var service = parseConfigTestConfig.createService(threadPool, clientManager)) {

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

    private void assertSuccessfulParse(SenderService service, Utils.PersistedConfig persistedConfig) throws Exception {
        var model = testCase.serviceParser.parseConfigs(new ServiceParserParams(service, persistedConfig, testConfiguration));

        if (persistedConfig.config().containsKey(ModelConfigurations.CHUNKING_SETTINGS)) {
            @SuppressWarnings("unchecked")
            var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(
                (Map<String, Object>) persistedConfig.config().get(ModelConfigurations.CHUNKING_SETTINGS)
            );
            assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
        }

        testConfiguration.commonConfig().assertModel(model, testCase.expectedTaskType, testCase.modelIncludesSecrets);
    }
}
