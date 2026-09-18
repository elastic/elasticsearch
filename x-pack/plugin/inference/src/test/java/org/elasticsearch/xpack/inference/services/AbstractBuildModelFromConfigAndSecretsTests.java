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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.junit.Assume;

import java.util.Arrays;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link InferenceService#buildModelFromConfigAndSecrets(ModelConfigurations, ModelSecrets)} using
 * parameterized tests.
 */
public abstract class AbstractBuildModelFromConfigAndSecretsTests extends AbstractInferenceServiceParameterizedTests {

    private final TestCase testCase;

    public AbstractBuildModelFromConfigAndSecretsTests(TestConfiguration testConfiguration, TestCase testCase) {
        super(testConfiguration);
        this.testCase = testCase;
    }

    public record TestCase(
        String description,
        Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigAndSecrets,
        TaskType expectedTaskType,
        boolean expectFailure
    ) {}

    private static class TestCaseBuilder {
        private final String description;
        private final Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigAndSecrets;
        private final TaskType expectedTaskType;
        private boolean expectFailure;

        TestCaseBuilder(
            String description,
            Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigurationsAndSecrets,
            TaskType expectedTaskType
        ) {
            this.description = description;
            this.createModelConfigAndSecrets = createModelConfigurationsAndSecrets;
            this.expectedTaskType = expectedTaskType;
        }

        public TestCaseBuilder expectFailure() {
            this.expectFailure = true;
            return this;
        }

        public TestCase build() {
            return new TestCase(description, createModelConfigAndSecrets, expectedTaskType, expectFailure);
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
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a completion model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.COMPLETION),
                        TaskType.COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a chat completion model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.CHAT_COMPLETION),
                        TaskType.CHAT_COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a text embedding model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.TEXT_EMBEDDING),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a sparse embedding model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.SPARSE_EMBEDDING),
                        TaskType.SPARSE_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates an embedding model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.EMBEDDING),
                        TaskType.EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a rerank model",
                        getTestConfigurationModelConfigAndSecretsFunction(TaskType.RERANK),
                        TaskType.RERANK
                    ).build() },
                { new TestCaseBuilder("Test failing building model for unsupported task type ", testConfiguration -> {
                    var configurationsMock = mock(ModelConfigurations.class);
                    when(configurationsMock.getTaskType()).thenReturn(TaskType.ANY);
                    return new Utils.ModelConfigAndSecrets(
                        configurationsMock,
                        testConfiguration.createModelSecrets(ConfigurationParseContext.PERSISTENT)
                    );
                },
                    // We expect failure, so the expected task type is irrelevant
                    null
                ).expectFailure().build() } }
        );
    }

    private static Function<TestConfiguration, Utils.ModelConfigAndSecrets> getTestConfigurationModelConfigAndSecretsFunction(
        TaskType taskType
    ) {
        return testConfiguration -> {
            var commonConfig = testConfiguration;
            var modelConfig = new ModelConfigurations(
                INFERENCE_ENTITY_ID,
                taskType,
                commonConfig.serviceName(),
                commonConfig.getServiceSettings(
                    commonConfig.createMinimalServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
                    taskType,
                    ConfigurationParseContext.PERSISTENT
                ),
                commonConfig.getEmptyTaskSettings(taskType)
            );
            return new Utils.ModelConfigAndSecrets(modelConfig, commonConfig.createModelSecrets(ConfigurationParseContext.PERSISTENT));
        };
    }

    public void testBuildModelFromConfigAndSecrets() {
        var commonConfig = testConfiguration;
        // If the service doesn't support the expected task type, then skip the test
        Assume.assumeTrue(
            "Service doesn't support task type",
            testCase.expectedTaskType == null || commonConfig.supportedTaskTypes().contains(testCase.expectedTaskType)
        );
        var modelConfigAndSecrets = testCase.createModelConfigAndSecrets.apply(testConfiguration);
        var service = commonConfig.createService(threadPool, clientManager);
        if (testCase.expectFailure) {
            assertFailedModelCreation(service, modelConfigAndSecrets);
        } else {
            assertSuccessfulModelCreation(service, modelConfigAndSecrets);
        }
    }

    private void assertSuccessfulModelCreation(InferenceService service, Utils.ModelConfigAndSecrets persistedConfig) {
        var model = service.buildModelFromConfigAndSecrets(persistedConfig.config(), persistedConfig.secrets());

        assertThat(model.getInferenceEntityId(), is(INFERENCE_ENTITY_ID));
        assertThat(model.getTaskSettings(), is(testConfiguration.getEmptyTaskSettings(testCase.expectedTaskType)));
        testConfiguration.assertModel(model, testCase.expectedTaskType, true, true, ConfigurationParseContext.PERSISTENT);
    }

    private void assertFailedModelCreation(InferenceService service, Utils.ModelConfigAndSecrets modelConfigAndSecrets) {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> service.buildModelFromConfigAndSecrets(modelConfigAndSecrets.config(), modelConfigAndSecrets.secrets())
        );

        assertThat(exception.getMessage(), containsString(Strings.format("service does not support task type [%s]", TaskType.ANY)));
    }
}
