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
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.junit.Assume;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base class for testing {@link InferenceService#parsePersistedConfig(UnparsedModel)} using
 * parameterized tests.
 */
public abstract class AbstractParsePersistedConfigTests extends AbstractInferenceServiceParameterizedTests {

    private final AbstractParsePersistedConfigTests.TestCase testCase;

    public AbstractParsePersistedConfigTests(TestConfiguration testConfig, TestCase testCase) {
        super(testConfig);
        this.testCase = testCase;
    }

    public record TestCase(
        String description,
        BiFunction<TestConfiguration, Boolean, Utils.PersistedConfig> createPersistedConfig,
        ServiceParser serviceParser,
        TaskType expectedTaskType,
        boolean modelIncludesSecrets,
        boolean expectFailure,
        boolean minimalSettings
    ) {}

    private record ServiceParserParams(InferenceService service, Utils.PersistedConfig persistedConfig, TestConfiguration testConfig) {}

    @FunctionalInterface
    private interface ServiceParser {
        Model parsePersistedConfigs(ServiceParserParams params);
    }

    private static class TestCaseBuilder {
        private final String description;
        private final BiFunction<TestConfiguration, Boolean, Utils.PersistedConfig> createPersistedConfig;
        private final ServiceParser serviceParser;
        private final TaskType expectedTaskType;
        private final boolean minimalSettings;
        private boolean modelIncludesSecrets;
        private boolean expectFailure;

        TestCaseBuilder(
            String description,
            BiFunction<TestConfiguration, Boolean, Utils.PersistedConfig> createPersistedConfig,
            ServiceParser serviceParser,
            TaskType expectedTaskType,
            boolean minimalSettings
        ) {
            this.description = description;
            this.createPersistedConfig = createPersistedConfig;
            this.serviceParser = serviceParser;
            this.expectedTaskType = expectedTaskType;
            this.minimalSettings = minimalSettings;
        }

        /**
         * Convenience constructor for test cases that use the default {@link TaskType} for
         * the {@link TestConfiguration}
         */
        TestCaseBuilder(String description, BiFunction<TestConfiguration, Boolean, Utils.PersistedConfig> createPersistedConfig) {
            this(description, createPersistedConfig, getDefaultServiceParser(), null, true);
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
            return new TestCase(
                description,
                createPersistedConfig,
                serviceParser,
                expectedTaskType,
                modelIncludesSecrets,
                expectFailure,
                minimalSettings
            );
        }
    }

    /**
     * To allow implementing classes to add additional parameters, this method is not annotated with {@link ParametersFactory}. Classes
     * implementing this abstract class which want to use the standard parameters should have:
     * <pre>
     *{@code @ParametersFactory
     * public static Iterable<TestCase[]> testParameters() {
     *     return parameters();
     * }
     * }
     * </pre>
     * The alternative to this approach is needing to add a new test class for every additional parameter (such as for each
     * {@link AmazonBedrockProvider}), which unnecessarily bloats the number of test files.
     * @return the default test cases
     */
    public static Iterable<TestCase[]> parameters() {
        return Arrays.asList(
            new TestCase[][] {
                // Test cases for parsePersistedConfig method
                {
                    new TestCaseBuilder(
                        "Test parsing text embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, false, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing text embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, true, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing text embedding persisted config with secrets returns model",
                        persistedConfig(TaskType.TEXT_EMBEDDING, false, true),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, false, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, true, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing sparse embedding persisted config with secrets returns model",
                        persistedConfig(TaskType.SPARSE_EMBEDDING, false, true),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embedding persisted config without chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, false, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embedding persisted config with chunking settings returns model",
                        persistedConfig(TaskType.EMBEDDING, true, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing embedding persisted config with secrets returns model",
                        persistedConfig(TaskType.EMBEDDING, false, true),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing completion persisted config returns model",
                        persistedConfig(TaskType.COMPLETION, false, false),
                        getServiceParser(TaskType.COMPLETION),
                        TaskType.COMPLETION,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing completion persisted config with secrets returns model",
                        persistedConfig(TaskType.COMPLETION, false, true),
                        getServiceParser(TaskType.COMPLETION),
                        TaskType.COMPLETION,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing chat completion persisted config returns model",
                        persistedConfig(TaskType.CHAT_COMPLETION, false, false),
                        getServiceParser(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing chat completion persisted config with secrets returns model",
                        persistedConfig(TaskType.CHAT_COMPLETION, false, true),
                        getServiceParser(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing rerank persisted config returns model",
                        persistedConfig(TaskType.RERANK, false, false),
                        getServiceParser(TaskType.RERANK),
                        TaskType.RERANK,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing rerank persisted config with secrets returns model",
                        persistedConfig(TaskType.RERANK, false, true),
                        getServiceParser(TaskType.RERANK),
                        TaskType.RERANK,
                        true
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config throws exception for unsupported task type",
                        (testConfig, minimalSettings) -> getPersistedConfigMap(
                            testConfig.createMinimalServiceSettingsMap(
                                testConfig.arbitraryTaskType(),
                                ConfigurationParseContext.PERSISTENT
                            ),
                            new HashMap<>(),
                            null
                        ),
                        params -> params.service.parsePersistedConfig(
                            new UnparsedModel(
                                INFERENCE_ENTITY_ID,
                                TaskType.ANY,
                                params.testConfig.serviceName(),
                                params.persistedConfig.config(),
                                params.persistedConfig.secrets()
                            )
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectFailure().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in config",
                        (testConfig, minimalSettings) -> {
                            var persistedConfigMap = getPersistedConfigMap(
                                testConfig.createMinimalServiceSettingsMap(
                                    testConfig.arbitraryTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                ),
                                testConfig.createMinimalTaskSettingsMap(testConfig.arbitraryTaskType()),
                                null
                            );
                            persistedConfigMap.config().put("extra_key", "value");
                            return persistedConfigMap;
                        }
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in service settings",
                        (testConfig, minimalSettings) -> {
                            var serviceSettings = testConfig.createMinimalServiceSettingsMap(
                                testConfig.arbitraryTaskType(),
                                ConfigurationParseContext.PERSISTENT
                            );
                            serviceSettings.put("extra_key", "value");

                            return getPersistedConfigMap(
                                serviceSettings,
                                testConfig.createMinimalTaskSettingsMap(testConfig.arbitraryTaskType()),
                                null
                            );
                        }
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in task settings",
                        (testConfig, minimalSettings) -> {
                            var taskSettingsMap = testConfig.createMinimalTaskSettingsMap(testConfig.arbitraryTaskType());
                            taskSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfig.createMinimalServiceSettingsMap(
                                    testConfig.arbitraryTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                ),
                                taskSettingsMap,
                                null
                            );
                        }
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in secret settings",
                        (testConfig, minimalSettings) -> {
                            var secretSettingsMap = testConfig.createSecretSettingsMap();
                            secretSettingsMap.put("extra_key", "value");

                            return getPersistedConfigMap(
                                testConfig.createMinimalServiceSettingsMap(
                                    testConfig.arbitraryTaskType(),
                                    ConfigurationParseContext.PERSISTENT
                                ),
                                testConfig.createMinimalTaskSettingsMap(testConfig.arbitraryTaskType()),
                                secretSettingsMap
                            );
                        }
                    ).withSecrets().build() },
                {
                    new TestCaseBuilder(
                        "Test parsing persisted config does not throw when an extra key exists in rate limit settings",
                        (testConfig, minimalSettings) -> {
                            var serviceSettings = testConfig.createMinimalServiceSettingsMap(
                                testConfig.arbitraryTaskType(),
                                ConfigurationParseContext.PERSISTENT
                            );
                            serviceSettings.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of("extra_key", "value")));

                            return getPersistedConfigMap(
                                serviceSettings,
                                testConfig.createMinimalTaskSettingsMap(testConfig.arbitraryTaskType()),
                                null
                            );
                        }
                    ).build() }, }
        );
    }

    private static BiFunction<TestConfiguration, Boolean, Utils.PersistedConfig> persistedConfig(
        TaskType taskType,
        boolean withChunking,
        boolean withSecrets
    ) {
        return (testConfig, minimalSettings) -> {
            Map<String, Object> serviceSettingsMap;
            Map<String, Object> taskSettingsMap;
            if (minimalSettings) {
                serviceSettingsMap = testConfig.createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT);
                taskSettingsMap = testConfig.createMinimalTaskSettingsMap(taskType);
            } else {
                serviceSettingsMap = testConfig.createAllServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT);
                taskSettingsMap = testConfig.createAllTaskSettingsMap(taskType);
            }
            return getPersistedConfigMap(
                serviceSettingsMap,
                taskSettingsMap,
                withChunking ? createRandomChunkingSettingsMap() : null,
                withSecrets ? testConfig.createSecretSettingsMap() : null
            );
        };
    }

    /**
     * Returns a {@link ServiceParser} function that calls {@link InferenceService#parsePersistedConfig(UnparsedModel)} with an
     * {@link UnparsedModel} using the specified {@link TaskType}
     *
     * @param taskType the {@link TaskType} of the {@link UnparsedModel} to create
     * @return a {@link ServiceParser} function
     */
    private static ServiceParser getServiceParser(TaskType taskType) {
        return params -> params.service.parsePersistedConfig(
            new UnparsedModel(
                INFERENCE_ENTITY_ID,
                taskType,
                params.testConfig.serviceName(),
                params.persistedConfig.config(),
                params.persistedConfig.secrets()
            )
        );
    }

    /**
     * Returns a {@link ServiceParser} function that calls {@link InferenceService#parsePersistedConfig(UnparsedModel)} with an
     * {@link UnparsedModel} using the default {@link TaskType} for the {@link TestConfiguration}
     *
     * @return a {@link ServiceParser} function
     */
    private static ServiceParser getDefaultServiceParser() {
        return params -> params.service.parsePersistedConfig(
            new UnparsedModel(
                INFERENCE_ENTITY_ID,
                params.testConfig.arbitraryTaskType(),
                params.testConfig.serviceName(),
                params.persistedConfig.config(),
                params.persistedConfig.secrets()
            )
        );
    }

    public void testPersistedConfig() throws Exception {
        TestConfiguration testConfiguration = this.testConfiguration;
        // If the service doesn't support the expected task type, then skip the test
        Assume.assumeTrue(
            "Service doesn't support task type",
            testCase.expectedTaskType == null || testConfiguration.supportedTaskTypes().contains(testCase.expectedTaskType)
        );

        var persistedConfig = testCase.createPersistedConfig.apply(this.testConfiguration, testCase.minimalSettings);
        try (var service = testConfiguration.createService(threadPool, clientManager)) {

            if (testCase.expectFailure) {
                assertFailedParse(service, persistedConfig);
            } else {
                assertSuccessfulParse(service, persistedConfig);
            }
        }
    }

    private void assertFailedParse(InferenceService service, Utils.PersistedConfig persistedConfig) {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> testCase.serviceParser.parsePersistedConfigs(new ServiceParserParams(service, persistedConfig, testConfiguration))
        );

        assertThat(exception.getMessage(), containsString(Strings.format("service does not support task type [%s]", TaskType.ANY)));
    }

    private void assertSuccessfulParse(InferenceService service, Utils.PersistedConfig persistedConfig) {
        var model = testCase.serviceParser.parsePersistedConfigs(new ServiceParserParams(service, persistedConfig, testConfiguration));
        assertThat(model.getInferenceEntityId(), is(INFERENCE_ENTITY_ID));

        var taskType = getTaskType(model);
        if (SenderService.CHUNKING_TASK_TYPES.contains(taskType)) {
            if (persistedConfig.config().containsKey(ModelConfigurations.CHUNKING_SETTINGS)) {
                @SuppressWarnings("unchecked")
                var expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(
                    (Map<String, Object>) persistedConfig.config().get(ModelConfigurations.CHUNKING_SETTINGS)
                );
                assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));
            }
        } else {
            assertThat(model.getConfigurations().getChunkingSettings(), nullValue());
        }

        testConfiguration.assertModel(
            model,
            taskType,
            testCase.modelIncludesSecrets,
            testCase.minimalSettings,
            ConfigurationParseContext.PERSISTENT
        );
    }

    // Use the expected task type from the test case if provided, otherwise use the task type from the model configurations
    private TaskType getTaskType(Model model) {
        return testCase.expectedTaskType != null ? testCase.expectedTaskType : model.getConfigurations().getTaskType();
    }
}
