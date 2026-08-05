/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.mockito.Mockito.mock;

public abstract class AbstractInferenceServiceParameterizedTests extends ESTestCase {
    static final String INFERENCE_ENTITY_ID = "inference_entity_id";
    protected final TestConfiguration testConfiguration;

    protected final MockWebServer webServer = new MockWebServer();
    protected ThreadPool threadPool;
    protected HttpClientManager clientManager;

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

    public AbstractInferenceServiceParameterizedTests(TestConfiguration testConfiguration) {
        this.testConfiguration = Objects.requireNonNull(testConfiguration);
    }

    /**
     * Configurations that are used for testing {@link InferenceService} implementations
     */
    public abstract static class TestConfiguration {

        private final TaskType arbitraryTaskType;
        private final EnumSet<TaskType> supportedTaskTypes;
        private final String serviceName;

        /**
         * @param supportedTaskTypes an {@link EnumSet} of all supported task types for the {@link InferenceService} being tested
         * @param serviceName the value returned from {@link InferenceService#name()} for the {@link InferenceService} being tested
         */
        public TestConfiguration(EnumSet<TaskType> supportedTaskTypes, String serviceName) {
            this.arbitraryTaskType = randomFrom(supportedTaskTypes);
            this.supportedTaskTypes = Objects.requireNonNull(supportedTaskTypes);
            this.serviceName = serviceName;
        }

        /**
         * Returns a {@link TaskType} from the set of supported {@link TaskType} for this {@link TestConfiguration}. The {@link TaskType}
         * returned is consistent for each instance of {@link TestConfiguration} but may change between test runs.
         * @return one of the supported {@link TaskType} for this {@link TestConfiguration}
         */
        public TaskType arbitraryTaskType() {
            return arbitraryTaskType;
        }

        public EnumSet<TaskType> supportedTaskTypes() {
            return supportedTaskTypes;
        }

        public String serviceName() {
            return serviceName;
        }

        protected abstract SenderService<?> createService(ThreadPool threadPool, HttpClientManager clientManager);

        /**
         * Returns a map containing only those service settings which are necessary to create a model with the given task type.
         *
         * @param taskType the task type of the model to create
         * @return a minimal map of service settings
         */
        protected abstract Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType);

        /**
         * Override as necessary for services which produce different service settings depending on the parse context.
         * <p>
         * This should be implemented to return a map containing only those service settings which are necessary to create a model with the
         * given task type.
         *
         * @param taskType the task type of the model to create
         * @param parseContext the parse context
         * @return a minimal map of service settings
         */
        protected Map<String, Object> createMinimalServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
            return createMinimalServiceSettingsMap(taskType);
        }

        /**
         * This should be implemented to return a map containing all valid service settings for a model with the given task type and
         * parse context.
         *
         * @param taskType     the task type of the model to create
         * @param parseContext the parse context, which may affect which fields are supported in the service settings
         * @return a map of all supported service settings for the given task type and parse context
         */
        protected abstract Map<String, Object> createAllServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext);

        /**
         * This should be implemented to return a {@link ServiceSettings} for the specified {@link TaskType} and
         * {@link ConfigurationParseContext} using the provided settings map.
         *
         * @param taskType the {@link TaskType} of the returned {@link ModelConfigurations}
         * @param settingsMap the map of settings to use to create the {@link ServiceSettings}
         * @return a {@link ModelConfigurations} with specified settings
         */
        protected abstract ServiceSettings getServiceSettings(
            Map<String, Object> settingsMap,
            TaskType taskType,
            ConfigurationParseContext context
        );

        /**
         * This should be implemented to return an empty {@link TaskSettings} implementation for the specified {@link TaskType}.
         *
         * @param taskType the {@link TaskType} of the returned {@link TaskSettings}
         * @return an {@link TaskSettings} of the correct class
         */
        protected abstract TaskSettings getEmptyTaskSettings(TaskType taskType);

        /**
         * This should be implemented to return the appropriate {@link ModelSecrets} implementation for the service being tested
         * @param context the {@link ConfigurationParseContext} to use when creating the {@link ModelSecrets}. May be ignored by
         *                some {@link ModelSecrets} implementations
         * @return {@link ModelSecrets} appropriate to the {@link InferenceService} being tested
         */
        protected abstract ModelSecrets createModelSecrets(ConfigurationParseContext context);

        /**
         * Override as necessary for services which have required task settings.
         * <p>
         * This should be implemented to return a <B>mutable</B> map containing any required task settings for the given task type.
         *
         * @param taskType the task type to create task settings for
         * @return a map containing required task settings for the given task type
         */
        protected Map<String, Object> createMinimalTaskSettingsMap(TaskType taskType) {
            return new HashMap<>();
        }

        /**
         * This should be implemented to return a <B>mutable</B> map containing all supported task settings for the given task type.
         *
         * @param taskType the task type to create task settings for
         * @return a map containing all supported task settings for the given task type
         */
        protected abstract Map<String, Object> createAllTaskSettingsMap(TaskType taskType);

        /**
         * This should be implemented to return a map containing the required secret settings for the {@link InferenceService} being tested
         * @return a map containing required secret settings for the {@link InferenceService} being tested
         */
        protected abstract Map<String, Object> createSecretSettingsMap();

        /**
         * This should be implemented to assert that the provided model matches what is expected for the given task type and settings.
         *
         * @param model the model to check
         * @param taskType the task type of the model
         * @param modelIncludesSecrets if true, the model should contain secret settings
         * @param minimalSettings if true, only required fields will be set, with all other fields using default values. If false, all
         *                        fields will be explicitly set
         */
        protected abstract void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, boolean minimalSettings);

        /**
         * Override as necessary for services which expect a different model to be created depending on the parse context.
         * <p>
         * This should be implemented to assert that the provided model matches what is expected for the given task type and settings.
         *
         * @param model the model to check
         * @param taskType the task type of the model
         * @param modelIncludesSecrets if true, the model should contain secret settings
         * @param minimalSettings if true, only required fields will be set, with all other fields using default values. If false, all
         *                        fields will be explicitly set
         * @param parseContext the parse context used to create the model
         */
        protected void assertModel(
            Model model,
            TaskType taskType,
            boolean modelIncludesSecrets,
            boolean minimalSettings,
            ConfigurationParseContext parseContext
        ) {
            assertModel(model, taskType, modelIncludesSecrets, minimalSettings);
        }

        /**
         * This should be implemented to return an {@link EnumSet} containing all {@link TaskType} that support streaming for
         * the {@link InferenceService} being tested
         * @return an {@link EnumSet} containing all {@link TaskType} that support streaming
         */
        protected abstract EnumSet<TaskType> supportedStreamingTasks();
    }
}
