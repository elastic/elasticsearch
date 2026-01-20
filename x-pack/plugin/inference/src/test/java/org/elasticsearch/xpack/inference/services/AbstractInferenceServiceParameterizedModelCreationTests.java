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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.Utils;
import org.junit.Assume;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

/**
 * Base class for testing inference services model creation using parameterized tests.
 */
public abstract class AbstractInferenceServiceParameterizedModelCreationTests extends AbstractInferenceServiceParameterizedTests {

    private final TestCase testCase;

    public AbstractInferenceServiceParameterizedModelCreationTests(TestConfiguration testConfiguration, TestCase testCase) {
        super(testConfiguration);
        this.testCase = testCase;
    }

    @Override
    public InferenceService createInferenceService() {
        return testConfiguration.commonConfig().createService(threadPool, clientManager);
    }

    public record TestCase(
        String description,
        Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigAndSecrets,
        ModelCreator modelCreator,
        TaskType expectedTaskType,
        boolean expectFailure
    ) {}

    private record ModelCreatorParams(
        SenderService service,
        Utils.ModelConfigAndSecrets modelConfigAndSecrets,
        TestConfiguration testConfiguration
    ) {}

    @FunctionalInterface
    private interface ModelCreator {
        Model buildModel(ModelCreatorParams params);
    }

    private static class TestCaseBuilder {
        private final String description;
        private final Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigAndSecrets;
        private final ModelCreator modelCreator;
        private final TaskType expectedTaskType;
        private boolean expectFailure;

        TestCaseBuilder(
            String description,
            Function<TestConfiguration, Utils.ModelConfigAndSecrets> createModelConfigurationsAndSecrets,
            ModelCreator modelCreator,
            TaskType expectedTaskType
        ) {
            this.description = description;
            this.createModelConfigAndSecrets = createModelConfigurationsAndSecrets;
            this.modelCreator = modelCreator;
            this.expectedTaskType = expectedTaskType;
        }

        public TestCaseBuilder expectFailure() {
            this.expectFailure = true;
            return this;
        }

        public TestCase build() {
            return new TestCase(description, createModelConfigAndSecrets, modelCreator, expectedTaskType, expectFailure);
        }
    }

    @ParametersFactory
    public static Iterable<TestCase[]> parameters() {
        return Arrays.asList(
            new TestCase[][] {
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a completion model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.COMPLETION),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a chat completion model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.CHAT_COMPLETION),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.CHAT_COMPLETION
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a text embedding model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.TEXT_EMBEDDING),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.TEXT_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a sparse embedding model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.SPARSE_EMBEDDING),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.SPARSE_EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates an embedding model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.EMBEDDING),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.EMBEDDING
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test building model from config and secrets creates a rerank model",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig().createModelConfigurations(TaskType.RERANK),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.RERANK
                    ).build() },
                {
                    new TestCaseBuilder(
                        "Test failing building model for unsupported task type ",
                        testConfiguration -> new Utils.ModelConfigAndSecrets(
                            testConfiguration.commonConfig()
                                .createModelConfigurations(testConfiguration.commonConfig().unsupportedTaskType()),
                            testConfiguration.commonConfig().createModelSecrets()
                        ),
                        params -> params.service.buildModelFromConfigAndSecrets(
                            params.modelConfigAndSecrets.config(),
                            params.modelConfigAndSecrets.secrets()
                        ),
                        TaskType.ANY
                    ).expectFailure().build() } }
        );
    }

    public void testBuildModelFromConfigAndSecrets() throws Exception {
        var commonConfig = testConfiguration.commonConfig();
        if (testCase.expectFailure == false) {
            // If the service doesn't support the expected task type, then skip the test
            Assume.assumeTrue("service doesn't support task type", commonConfig.supportedTaskTypes().contains(testCase.expectedTaskType));

            // If the test case expects success, verify model creation success
            buildModelAndAssert(commonConfig, this::assertSuccessfulModelCreation);
        } else {
            // If the test case expects failure, ignore expected task type and verify model creation failure
            buildModelAndAssert(commonConfig, this::assertFailedModelCreation);
        }
    }

    private void buildModelAndAssert(
        CommonConfig commonConfig,
        BiConsumer<SenderService, Utils.ModelConfigAndSecrets> assertionBiConsumer
    ) {
        var modelConfigAndSecrets = testCase.createModelConfigAndSecrets.apply(testConfiguration);
        var service = commonConfig.createService(threadPool, clientManager);
        assertionBiConsumer.accept(service, modelConfigAndSecrets);
    }

    private void assertFailedModelCreation(SenderService service, Utils.ModelConfigAndSecrets modelConfigAndSecrets) {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> testCase.modelCreator.buildModel(new ModelCreatorParams(service, modelConfigAndSecrets, testConfiguration))
        );

        assertThat(
            exception.getMessage(),
            containsString(
                Strings.format("service does not support task type [%s]", testConfiguration.commonConfig().unsupportedTaskType())
            )
        );
    }

    private void assertSuccessfulModelCreation(SenderService service, Utils.ModelConfigAndSecrets persistedConfig) {
        var model = testCase.modelCreator.buildModel(new ModelCreatorParams(service, persistedConfig, testConfiguration));

        testConfiguration.commonConfig().assertModel(model, testCase.expectedTaskType, true, ConfigurationParseContext.PERSISTENT);
    }
}
