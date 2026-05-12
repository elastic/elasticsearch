/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.junit.Assume;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Base class for testing {@link InferenceService#parseRequestConfig(String, TaskType, Map, ActionListener)} using parameterized tests.
 */
public abstract class AbstractParseRequestConfigTests extends AbstractInferenceServiceParameterizedTests {

    private static final String EXTRA_KEY = "extra_key";
    private static final String EXTRA_VALUE = "value";

    private final AbstractParseRequestConfigTests.TestCase testCase;

    public AbstractParseRequestConfigTests(TestConfiguration testConfiguration, TestCase testCase) {
        super(testConfiguration);
        this.testCase = testCase;
    }

    public record TestCase(
        String description,
        BiFunction<TestConfiguration, Boolean, Map<String, Object>> createRequestConfig,
        ServiceParser serviceParser,
        TaskType expectedTaskType,
        TriConsumer<TestConfiguration, Boolean, TestPlainActionFuture<Model>> expectedExceptionAssertions,
        boolean minimalSettings
    ) {}

    private record ServiceParserParams(InferenceService service, Map<String, Object> requestConfig, TestConfiguration testConfiguration) {}

    @FunctionalInterface
    private interface ServiceParser {
        void parseRequestConfigs(ServiceParserParams params, ActionListener<Model> listener);
    }

    private static class TestCaseBuilder {
        private final String description;
        private final BiFunction<TestConfiguration, Boolean, Map<String, Object>> createRequestConfig;
        private final ServiceParser serviceParser;
        private final TaskType expectedTaskType;
        private TriConsumer<TestConfiguration, Boolean, TestPlainActionFuture<Model>> expectedExceptionAssertions;
        private final boolean minimalSettings;

        TestCaseBuilder(
            String description,
            BiFunction<TestConfiguration, Boolean, Map<String, Object>> createRequestConfig,
            ServiceParser serviceParser,
            TaskType expectedTaskType,
            boolean minimalSettings
        ) {
            this.description = description;
            this.createRequestConfig = createRequestConfig;
            this.serviceParser = serviceParser;
            this.expectedTaskType = expectedTaskType;
            this.minimalSettings = minimalSettings;
        }

        public TestCaseBuilder expectException(
            TriConsumer<TestConfiguration, Boolean, TestPlainActionFuture<Model>> expectedExceptionAssertions
        ) {
            this.expectedExceptionAssertions = expectedExceptionAssertions;
            return this;
        }

        public TestCase build() {
            return new TestCase(
                description,
                createRequestConfig,
                serviceParser,
                expectedTaskType,
                expectedExceptionAssertions,
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
                // Test cases for parseRequestConfig method
                {
                    new TestCaseBuilder(
                        "Test parsing minimal text embedding request config without chunking settings returns model",
                        requestConfig(TaskType.TEXT_EMBEDDING, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal text embedding request config with chunking settings returns model",
                        requestConfig(TaskType.TEXT_EMBEDDING, true),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings text embedding request config",
                        requestConfig(TaskType.TEXT_EMBEDDING, false),
                        getServiceParser(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal sparse embedding request config without chunking settings returns model",
                        requestConfig(TaskType.SPARSE_EMBEDDING, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal sparse embedding request config with chunking settings returns model",
                        requestConfig(TaskType.SPARSE_EMBEDDING, true),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings sparse embedding request config",
                        requestConfig(TaskType.SPARSE_EMBEDDING, false),
                        getServiceParser(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal embedding request config without chunking settings returns model",
                        requestConfig(TaskType.EMBEDDING, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal embedding request config with chunking settings returns model",
                        requestConfig(TaskType.EMBEDDING, true),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings embedding request config",
                        requestConfig(TaskType.EMBEDDING, false),
                        getServiceParser(TaskType.EMBEDDING),
                        TaskType.EMBEDDING,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal completion request config returns model",
                        requestConfig(TaskType.COMPLETION, false),
                        getServiceParser(TaskType.COMPLETION),
                        TaskType.COMPLETION,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings completion request config returns model",
                        requestConfig(TaskType.COMPLETION, false),
                        getServiceParser(TaskType.COMPLETION),
                        TaskType.COMPLETION,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal chat completion request config returns model",
                        requestConfig(TaskType.CHAT_COMPLETION, false),
                        getServiceParser(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings chat completion request config returns model",
                        requestConfig(TaskType.CHAT_COMPLETION, false),
                        getServiceParser(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing minimal rerank request config returns model",
                        requestConfig(TaskType.RERANK, false),
                        getServiceParser(TaskType.RERANK),
                        TaskType.RERANK,
                        true
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing all settings rerank request config returns model",
                        requestConfig(TaskType.RERANK, false),
                        getServiceParser(TaskType.RERANK),
                        TaskType.RERANK,
                        false
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing request config throws exception for unsupported task type",
                        (testConfig, minimalSettings) -> requestConfig(testConfig, testConfig.arbitraryTaskType(), false, minimalSettings),
                        (params, listener) -> params.service.parseRequestConfig(
                            INFERENCE_ENTITY_ID,
                            TaskType.ANY,
                            params.requestConfig,
                            listener
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectException((t, usesParserForTaskSettings, listener) -> assertUnsupportedTaskTypeException(t, listener))
                        .build() },
                {
                    new TestCaseBuilder(
                        "Test parsing request config throws when an extra key exists in config",
                        (testConfig, minimalSettings) -> {
                            var requestConfigMap = requestConfig(testConfig, testConfig.arbitraryTaskType(), false, minimalSettings);
                            requestConfigMap.put(EXTRA_KEY, EXTRA_VALUE);
                            return requestConfigMap;
                        },
                        (params, listener) -> params.service.parseRequestConfig(
                            INFERENCE_ENTITY_ID,
                            params.testConfiguration.arbitraryTaskType(),
                            params.requestConfig,
                            listener
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectException((t, usesParserForTaskSettings, listener) -> assertNonParserExtraValueException(t, listener))
                        .build() },
                {
                    new TestCaseBuilder(
                        "Test parsing request config throws when an extra key exists in service settings",
                        (testConfig, minimalSettings) -> {
                            var serviceSettings = testConfig.createMinimalServiceSettingsMap(
                                testConfig.arbitraryTaskType(),
                                ConfigurationParseContext.REQUEST
                            );
                            serviceSettings.put(EXTRA_KEY, EXTRA_VALUE);

                            return getRequestConfigMap(
                                serviceSettings,
                                testConfig.createAllTaskSettingsMap(testConfig.arbitraryTaskType()),
                                testConfig.createSecretSettingsMap()
                            );
                        },
                        (params, listener) -> params.service.parseRequestConfig(
                            INFERENCE_ENTITY_ID,
                            params.testConfiguration.arbitraryTaskType(),
                            params.requestConfig,
                            listener
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectException((t, usesParserForTaskSettings, listener) -> assertNonParserExtraValueException(t, listener))
                        .build() },
                {
                    new TestCaseBuilder(
                        "Test parsing request config throws when an extra key exists in task settings",
                        (testConfig, minimalSettings) -> {
                            var taskSettingsMap = testConfig.createAllTaskSettingsMap(testConfig.arbitraryTaskType());
                            taskSettingsMap.put(EXTRA_KEY, EXTRA_VALUE);

                            return getRequestConfigMap(
                                testConfig.createMinimalServiceSettingsMap(
                                    testConfig.arbitraryTaskType(),
                                    ConfigurationParseContext.REQUEST
                                ),
                                taskSettingsMap,
                                testConfig.createSecretSettingsMap()
                            );
                        },
                        (params, listener) -> params.service.parseRequestConfig(
                            INFERENCE_ENTITY_ID,
                            params.testConfiguration.arbitraryTaskType(),
                            params.requestConfig,
                            listener
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectException((t, usesParserForTaskSettings, listener) -> {
                        if (usesParserForTaskSettings) {
                            assertParserExtraValueException(t, listener);
                        } else {
                            assertNonParserExtraValueException(t, listener);
                        }
                    }).build() },
                {
                    new TestCaseBuilder(
                        "Test parsing request config throws when an extra key exists in rate limit settings",
                        (testConfig, minimalSettings) -> {
                            var serviceSettings = testConfig.createMinimalServiceSettingsMap(
                                testConfig.arbitraryTaskType(),
                                ConfigurationParseContext.REQUEST
                            );
                            serviceSettings.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(EXTRA_KEY, EXTRA_VALUE)));

                            return getRequestConfigMap(
                                serviceSettings,
                                testConfig.createAllTaskSettingsMap(testConfig.arbitraryTaskType()),
                                testConfig.createSecretSettingsMap()
                            );
                        },
                        (params, listener) -> params.service.parseRequestConfig(
                            INFERENCE_ENTITY_ID,
                            params.testConfiguration.arbitraryTaskType(),
                            params.requestConfig,
                            listener
                        ),
                        // We expect failure, so the expected task type is irrelevant
                        null,
                        true
                    ).expectException((t, usesParserForTaskSettings, listener) -> assertRateLimitExtraValueException(t, listener))
                        .build() } }
        );
    }

    private static void assertUnsupportedTaskTypeException(TestConfiguration t, TestPlainActionFuture<Model> listener) {
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is(Strings.format("The [%s] service does not support task type [any]", t.serviceName())));
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
    }

    private static void assertNonParserExtraValueException(TestConfiguration t, TestPlainActionFuture<Model> listener) {
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Configuration contains settings [{%s=%s}] unknown to the [%s] service",
                    EXTRA_KEY,
                    EXTRA_VALUE,
                    t.serviceName()
                )
            )
        );
    }

    private static void assertRateLimitExtraValueException(TestConfiguration t, TestPlainActionFuture<Model> listener) {
        var exception = expectThrows(ValidationException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.validationErrors(), hasSize(1));
        assertThat(
            exception.validationErrors().getFirst(),
            is(Strings.format("Rate limit settings contain unknown entries [{%s=%s}]", EXTRA_KEY, EXTRA_VALUE))
        );
    }

    private static void assertParserExtraValueException(TestConfiguration t, TestPlainActionFuture<Model> listener) {
        var exception = expectThrows(XContentParseException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), containsString(Strings.format("unknown field [%s]", EXTRA_KEY)));
    }

    private static Map<String, Object> requestConfig(
        TestConfiguration testConfiguration,
        TaskType taskType,
        boolean withChunking,
        boolean minimalSettings
    ) {
        Map<String, Object> serviceSettingsMap;
        Map<String, Object> taskSettingsMap;

        if (minimalSettings) {
            serviceSettingsMap = testConfiguration.createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            taskSettingsMap = testConfiguration.createMinimalTaskSettingsMap(taskType);
        } else {
            serviceSettingsMap = testConfiguration.createAllServiceSettingsMap(taskType, ConfigurationParseContext.REQUEST);
            taskSettingsMap = testConfiguration.createAllTaskSettingsMap(taskType);
        }

        var requestConfigMap = getRequestConfigMap(serviceSettingsMap, taskSettingsMap, testConfiguration.createSecretSettingsMap());

        if (withChunking) {
            requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, createRandomChunkingSettingsMap());
        }
        return requestConfigMap;
    }

    private static BiFunction<TestConfiguration, Boolean, Map<String, Object>> requestConfig(TaskType taskType, boolean withChunking) {
        return (testConfig, minimalSettings) -> requestConfig(testConfig, taskType, withChunking, minimalSettings);
    }

    /**
     * Returns a {@link ServiceParser} function that calls
     * {@link InferenceService#parseRequestConfig(String, TaskType, Map, ActionListener)} using the specified {@link TaskType}
     *
     * @param taskType the {@link TaskType} to use
     * @return a {@link ServiceParser} function
     */
    private static ServiceParser getServiceParser(TaskType taskType) {
        return (params, listener) -> params.service.parseRequestConfig(INFERENCE_ENTITY_ID, taskType, params.requestConfig, listener);
    }

    public void testRequestConfig() throws Exception {
        TestConfiguration testConfiguration = this.testConfiguration;
        // If the service doesn't support the expected task type, then skip the test
        Assume.assumeTrue(
            "Service doesn't support task type",
            testCase.expectedTaskType == null || testConfiguration.supportedTaskTypes().contains(testCase.expectedTaskType)
        );

        var listener = new TestPlainActionFuture<Model>();

        var requestConfig = testCase.createRequestConfig.apply(this.testConfiguration, testCase.minimalSettings);
        ChunkingSettings expectedChunkingSettings = null;
        if (SenderService.CHUNKING_TASK_TYPES.contains(testCase.expectedTaskType)) {
            // Determine the expected chunking settings before parsing the request, otherwise they get removed from the map
            @SuppressWarnings("unchecked")
            var chunkingSettingsMap = (Map<String, Object>) requestConfig.getOrDefault(ModelConfigurations.CHUNKING_SETTINGS, Map.of());
            expectedChunkingSettings = ChunkingSettingsBuilder.fromMap(chunkingSettingsMap);
        }
        try (var service = testConfiguration.createService(threadPool, clientManager)) {
            testCase.serviceParser.parseRequestConfigs(new ServiceParserParams(service, requestConfig, this.testConfiguration), listener);

            if (testCase.expectedExceptionAssertions != null) {
                assertFailedParse(service, listener);
            } else {
                assertSuccessfulParse(listener, expectedChunkingSettings);
            }
        }
    }

    private void assertFailedParse(InferenceService service, TestPlainActionFuture<Model> listener) {
        testCase.expectedExceptionAssertions.apply(testConfiguration, service.usesParserForTaskSettings(), listener);
    }

    private void assertSuccessfulParse(TestPlainActionFuture<Model> listener, ChunkingSettings expectedChunkingSettings) {
        var model = listener.actionGet(TEST_REQUEST_TIMEOUT);

        assertThat(model.getInferenceEntityId(), is(INFERENCE_ENTITY_ID));
        assertThat(model.getConfigurations().getChunkingSettings(), is(expectedChunkingSettings));

        testConfiguration.assertModel(
            model,
            // Use the expected task type from the test case if provided, otherwise use the task type from the model configurations
            testCase.expectedTaskType != null ? testCase.expectedTaskType : model.getConfigurations().getTaskType(),
            true,
            testCase.minimalSettings,
            ConfigurationParseContext.REQUEST
        );
    }
}
