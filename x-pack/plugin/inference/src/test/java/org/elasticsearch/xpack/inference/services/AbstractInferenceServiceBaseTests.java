/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.mockito.Mockito.mock;

public abstract class AbstractInferenceServiceBaseTests extends InferenceServiceTestCase {
    protected final TestConfiguration testConfiguration;

    protected final MockWebServer webServer = new MockWebServer();
    protected ThreadPool threadPool;
    protected HttpClientManager clientManager;
    protected AbstractInferenceServiceParameterizedTests.TestCase testCase;

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

    public AbstractInferenceServiceBaseTests(TestConfiguration testConfiguration) {
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

            public TestConfiguration.Builder enableUpdateModelTests(UpdateModelConfiguration updateModelConfiguration) {
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

        private final TaskType targetTaskType;
        private final TaskType unsupportedTaskType;
        private final EnumSet<TaskType> supportedTaskTypes;

        public CommonConfig(TaskType targetTaskType, @Nullable TaskType unsupportedTaskType, EnumSet<TaskType> supportedTaskTypes) {
            this.targetTaskType = Objects.requireNonNull(targetTaskType);
            this.unsupportedTaskType = unsupportedTaskType;
            this.supportedTaskTypes = Objects.requireNonNull(supportedTaskTypes);
        }

        public TaskType targetTaskType() {
            return targetTaskType;
        }

        public TaskType unsupportedTaskType() {
            return unsupportedTaskType;
        }

        public EnumSet<TaskType> supportedTaskTypes() {
            return supportedTaskTypes;
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

        /**
         * Override this method if the service support reranking. This method won't be called if the service doesn't support reranking.
         */
        protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
            fail("Reranking services should override this test method to verify window size");
        }
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

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        testConfiguration.commonConfig.assertRerankerWindowSize(rerankingInferenceService);
    }
}
