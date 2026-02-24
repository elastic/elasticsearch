/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModel;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankModelTests;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.TaskType.COMPLETION;
import static org.elasticsearch.inference.TaskType.RERANK;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.getRequestConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankServiceSettingsTests.getServiceSettingsMap;
import static org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettingsTests.getTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class MixedbreadServiceTests extends AbstractInferenceServiceTests {
    public static final String UNKNOWN_SETTINGS_EXCEPTION =
        "Configuration contains settings [{extra_key=value}] unknown to the [mixedbread] service";
    public static final Boolean RETURN_DOCUMENTS_TRUE = true;
    public static final Boolean RETURN_DOCUMENTS_FALSE = false;
    public static final String DEFAULT_RERANK_URL = "https://api.mixedbread.com/v1/reranking";

    private static final String INFERENCE_ID_VALUE = "id";
    private static final String MODEL_NAME_VALUE = "modelName";
    private static final String API_KEY = "secret";
    private static final String QUERY_VALUE = "query";
    private static final Integer TOP_N = 3;
    private static final Integer REQUESTS_PER_MINUTE = 3;
    private static final Boolean STREAM = false;
    private static final List<String> INPUT = List.of("candidate1", "candidate2", "candidate3");

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    public MixedbreadServiceTests() {
        super(createTestConfiguration());
    }

    public static TestConfiguration createTestConfiguration() {
        return new TestConfiguration.Builder(new CommonConfig(RERANK, COMPLETION, EnumSet.of(RERANK)) {

            @Override
            protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                return MixedbreadServiceTests.createService(threadPool, clientManager);
            }

            @Override
            protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                return MixedbreadRerankServiceSettingsTests.getServiceSettingsMap(MODEL_NAME_VALUE, null);
            }

            @Override
            protected ModelConfigurations createModelConfigurations(TaskType taskType) {
                return switch (taskType) {
                    case RERANK -> new ModelConfigurations(
                        INFERENCE_ID_VALUE,
                        taskType,
                        MixedbreadService.NAME,
                        MixedbreadRerankServiceSettings.fromMap(
                            createServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
                            ConfigurationParseContext.PERSISTENT
                        ),
                        MixedbreadRerankTaskSettings.EMPTY_SETTINGS
                    );
                    // Completion is not supported, but in order to test unsupported task types it is included here
                    case COMPLETION -> new ModelConfigurations(
                        INFERENCE_ID_VALUE,
                        taskType,
                        MixedbreadService.NAME,
                        mock(ServiceSettings.class),
                        mock(TaskSettings.class)
                    );
                    default -> throw new IllegalStateException("Unexpected value: " + taskType);
                };
            }

            @Override
            protected ModelSecrets createModelSecrets() {
                return new ModelSecrets(DefaultSecretSettings.fromMap(createSecretSettingsMap()));
            }

            @Override
            protected Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
                return MixedbreadServiceTests.createServiceSettingsMap(taskType, parseContext);
            }

            @Override
            protected Map<String, Object> createTaskSettingsMap(TaskType taskType) {
                if (taskType.equals(RERANK)) {
                    return MixedbreadRerankTaskSettingsTests.getTaskSettingsMap(null, null);
                }
                return createTaskSettingsMap();
            }

            @Override
            protected Map<String, Object> createTaskSettingsMap() {
                return new HashMap<>();
            }

            @Override
            protected Map<String, Object> createSecretSettingsMap() {
                return MixedbreadServiceTests.createSecretSettingsMap();
            }

            @Override
            protected void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
                MixedbreadServiceTests.assertModel(model, taskType, modelIncludesSecrets);
            }

            @Override
            protected EnumSet<TaskType> supportedStreamingTasks() {
                return EnumSet.noneOf(TaskType.class);
            }

            @Override
            protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
                assertThat(rerankingInferenceService.rerankerWindowSize(MODEL_NAME_VALUE), Matchers.is(22000));
            }
        }).build();
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        if (Objects.requireNonNull(taskType) == RERANK) {
            assertRerankModel(model, modelIncludesSecrets);
        } else {
            fail("unexpected task type [" + taskType + "]");
        }
    }

    private static MixedbreadModel assertCommonModelFields(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(MixedbreadModel.class));

        var mixedbreadModel = (MixedbreadModel) model;
        assertThat(mixedbreadModel.getServiceSettings().modelId(), Matchers.is(MODEL_NAME_VALUE));
        if (modelIncludesSecrets) {
            assertThat(mixedbreadModel.getSecretSettings().apiKey(), Matchers.is(new SecureString(API_KEY.toCharArray())));
        }
        return mixedbreadModel;
    }

    private static void assertRerankModel(Model model, boolean modelIncludesSecrets) {
        var mixedbreadModel = assertCommonModelFields(model, modelIncludesSecrets);
        assertThat(mixedbreadModel.getTaskSettings(), Matchers.is(MixedbreadRerankTaskSettings.EMPTY_SETTINGS));
        assertThat(mixedbreadModel.getTaskType(), Matchers.is(RERANK));
    }

    public static SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        return new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
        return MixedbreadRerankServiceSettingsTests.getServiceSettingsMap(MODEL_NAME_VALUE, null);
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of("api_key", API_KEY));
    }

    public void testBuildModelFromConfigAndSecrets_UnsupportedTaskType() throws IOException {
        var modelConfigurations = new ModelConfigurations(
            INFERENCE_ID_VALUE,
            TaskType.COMPLETION,
            MixedbreadService.NAME,
            mock(ServiceSettings.class)
        );
        try (var inferenceService = createInferenceService()) {
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.buildModelFromConfigAndSecrets(modelConfigurations, mock(ModelSecrets.class))
            );
            assertThat(
                thrownException.getMessage(),
                CoreMatchers.is(
                    org.elasticsearch.core.Strings.format(
                        """
                            Failed to parse stored model [%s] for [%s] service, error: [The [%s] service does not support task type [%s]]. \
                            Please delete and add the service again""",
                        INFERENCE_ID_VALUE,
                        MixedbreadService.NAME,
                        MixedbreadService.NAME,
                        TaskType.COMPLETION
                    )
                )
            );
        }
    }

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testParseRequestConfig_createsRerankModel() throws IOException {
        try (var service = createMixedbreadService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(
                    getServiceSettingsMap(modelName, requestsPerMinute),
                    getTaskSettingsMap(topN, returnDocuments),
                    getSecretSettingsMap(apiKey)
                ),
                modelListener
            );

            var rerankModel = (MixedbreadRerankModel) modelListener.actionGet();

            assertThat(rerankModel.getSecretSettings().apiKey().toString(), is(apiKey));
            assertRerankModelSettings(
                rerankModel,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                apiKey,
                new MixedbreadRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParseRequestConfig_onlyRequiredSettings_createsRerankModel() throws IOException {
        try (var service = createMixedbreadService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var modelListener = new PlainActionFuture<Model>();

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(getServiceSettingsMap(modelName), Map.of(), getSecretSettingsMap(apiKey)),
                modelListener
            );

            var rerankModel = (MixedbreadRerankModel) modelListener.actionGet();

            assertThat(rerankModel.getSecretSettings().apiKey().toString(), is(apiKey));
            assertRerankModelSettings(
                modelListener.actionGet(),
                modelName,
                MixedbreadRerankServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS,
                apiKey,
                MixedbreadRerankTaskSettings.EMPTY_SETTINGS
            );

        }
    }

    public void testParsePersistedConfigWithSecrets_createsRerankModel() throws IOException {
        try (var service = createMixedbreadService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, requestsPerMinute),
                getTaskSettingsMap(topN, returnDocuments),
                getSecretSettingsMap(apiKey)
            );

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ID_VALUE,
                TaskType.RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model.getSecretSettings().apiKey().toString(), is(apiKey));
            assertRerankModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                apiKey,
                new MixedbreadRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParsePersistedConfigWithSecrets_onlyRequiredSettings_createsRerankModel() throws IOException {
        try (var service = createMixedbreadService()) {
            var modelName = randomAlphanumericOfLength(8);
            var apiKey = randomAlphanumericOfLength(8);

            var persistedConfig = getPersistedConfigMap(getServiceSettingsMap(modelName, null), Map.of(), getSecretSettingsMap(apiKey));

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ID_VALUE,
                TaskType.RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model.getSecretSettings().apiKey().toString(), is(apiKey));
            assertRerankModelSettings(
                model,
                modelName,
                MixedbreadRerankServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS,
                apiKey,
                MixedbreadRerankTaskSettings.EMPTY_SETTINGS
            );
        }
    }

    public void testParsePersistedConfig_createsRerankModel() throws IOException {
        try (var service = createMixedbreadService()) {
            var modelName = randomAlphanumericOfLength(8);
            var requestsPerMinute = randomNonNegativeInt();
            var topN = randomNonNegativeInt();
            var returnDocuments = randomBoolean();

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, requestsPerMinute),
                getTaskSettingsMap(topN, returnDocuments),
                null
            );

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, TaskType.RERANK, persistedConfig.config());

            assertRerankModelSettings(
                model,
                modelName,
                new RateLimitSettings(requestsPerMinute),
                "",
                new MixedbreadRerankTaskSettings(topN, returnDocuments)
            );
        }
    }

    public void testParseRequestConfig_NoModelId_ThrowsException() throws IOException {
        try (var service = createMixedbreadService()) {
            ActionListener<Model> modelListener = ActionListener.wrap(
                model -> fail("Expected exception, but got model: " + model),
                exception -> {
                    assertThat(exception, instanceOf(ValidationException.class));
                    assertThat(
                        exception.getMessage(),
                        Matchers.is("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
                    );
                }
            );

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TaskType.RERANK,
                getRequestConfigMap(
                    getServiceSettingsMap(null, REQUESTS_PER_MINUTE),
                    getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE),
                    getSecretSettingsMap(API_KEY)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankSecretSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var secretSettings = getSecretSettingsMap(API_KEY);
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE),
                getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE),
                secretSettings
            );

            assertThrowsExceptionWhenAnExtraKeyExists(service, config);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankServiceSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE),
                getSecretSettingsMap(API_KEY)
            );

            assertThrowsExceptionWhenAnExtraKeyExists(service, config);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankTaskSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
            taskSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE),
                taskSettings,
                getSecretSettingsMap(API_KEY)
            );

            assertThrowsExceptionWhenAnExtraKeyExists(service, config);
        }
    }

    private static void assertThrowsExceptionWhenAnExtraKeyExists(MixedbreadService service, Map<String, Object> config) {
        ActionListener<Model> modelVerificationListener = ActionListener.wrap(
            model -> fail("Expected exception, but got model: " + model),
            exception -> {
                assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                assertThat(exception.getMessage(), Matchers.is(UNKNOWN_SETTINGS_EXCEPTION));
            }
        );

        service.parseRequestConfig(INFERENCE_ID_VALUE, RERANK, config, modelVerificationListener);
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            secretSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            serviceSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            secretSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ID_VALUE,
                RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(API_KEY));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            serviceSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ID_VALUE,
                RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(API_KEY));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(MODEL_NAME_VALUE, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(API_KEY);

        try (var service = createMixedbreadService()) {
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfigWithSecrets(
                INFERENCE_ID_VALUE,
                RERANK,
                persistedConfig.config(),
                persistedConfig.secrets()
            );

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(MODEL_NAME_VALUE));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(API_KEY));
        }
    }

    public void testInfer_Rerank_UnauthorisedResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            var model = MixedbreadRerankModelTests.createModel(MODEL_NAME_VALUE, API_KEY, TOP_N, RETURN_DOCUMENTS_FALSE, getUrl(webServer));

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                QUERY_VALUE,
                null,
                null,
                List.of("candidate1", "candidate2"),
                STREAM,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var error = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(error.getMessage(), containsString("Received an authentication error status code for request"));
            assertThat(error.getMessage(), containsString("Unauthorized"));
            assertThat(webServer.requests(), hasSize(1));
        }
    }

    public void testInfer_Rerank_NoReturnDocuments_NoTopN() throws IOException {
        String responseJson = """
            {
                "usage": {
                        "prompt_tokens": 162,
                        "total_tokens": 162,
                        "completion_tokens": 0
                },
                "model": "modelName",
                "data": [
                    {
                        "index": 0,
                        "score": 0.98291015625,
                        "object": "rank_result"
                    },
                    {
                        "index": 2,
                        "score": 0.61962890625,
                        "object": "rank_result"
                    },
                    {
                        "index": 3,
                        "score": 0.3642578125,
                        "object": "rank_result"
                    }
                ],
                "object": "list",
                "return_input": false
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = MixedbreadRerankModelTests.createModel(MODEL_NAME_VALUE, API_KEY, null, RETURN_DOCUMENTS_FALSE, getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                QUERY_VALUE,
                null,
                null,
                INPUT,
                STREAM,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("index", 0, "relevance_score", 0.98291016F)),
                            Map.of("ranked_doc", Map.of("index", 2, "relevance_score", 0.6196289F)),
                            Map.of("ranked_doc", Map.of("index", 3, "relevance_score", 0.3642578F))
                        )
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(Map.of("query", QUERY_VALUE, "input", INPUT, "model", MODEL_NAME_VALUE, "return_input", RETURN_DOCUMENTS_FALSE))
            );
        }
    }

    public void testInfer_Rerank_ReturnDocumentsNull_NoTopN() throws IOException {
        String responseJson = """
            {
                "usage": {
                        "prompt_tokens": 162,
                        "total_tokens": 162,
                        "completion_tokens": 0
                },
                "model": "modelName",
                "data": [
                    {
                        "index": 0,
                        "score": 0.98291015625,
                        "input": "candidate3",
                        "object": "rank_result"
                    },
                    {
                        "index": 2,
                        "score": 0.61962890625,
                        "input": "candidate2",
                        "object": "rank_result"
                    },
                    {
                        "index": 3,
                        "score": 0.3642578125,
                        "input": "candidate1",
                        "object": "rank_result"
                    }
                ],
                "object": "list"
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = MixedbreadRerankModelTests.createModel(MODEL_NAME_VALUE, API_KEY, null, null, getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                QUERY_VALUE,
                null,
                null,
                INPUT,
                STREAM,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("index", 0, "relevance_score", 0.98291015625F, "text", "candidate3")),
                            Map.of("ranked_doc", Map.of("index", 2, "relevance_score", 0.61962890625F, "text", "candidate2")),
                            Map.of("ranked_doc", Map.of("index", 3, "relevance_score", 0.3642578125F, "text", "candidate1"))
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(requestMap, is(Map.of("query", QUERY_VALUE, "input", INPUT, "model", MODEL_NAME_VALUE)));

        }
    }

    public void testInfer_Rerank_ReturnDocuments_TopN() throws IOException {
        String responseJson = """
            {
                "usage": {
                        "prompt_tokens": 162,
                        "total_tokens": 162,
                        "completion_tokens": 0
                },
                "model": "modelName",
                "data": [
                    {
                        "index": 0,
                        "score": 0.98291015625,
                        "input": "candidate3",
                        "object": "rank_result"
                    },
                    {
                        "index": 2,
                        "score": 0.61962890625,
                        "input": "candidate2",
                        "object": "rank_result"
                    },
                    {
                        "index": 3,
                        "score": 0.3642578125,
                        "input": "candidate1",
                        "object": "rank_result"
                    }
                ],
                "object": "list",
                "top_k": 3,
                "return_input": true
            }
            """;
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = MixedbreadRerankModelTests.createModel(MODEL_NAME_VALUE, API_KEY, TOP_N, RETURN_DOCUMENTS_TRUE, getUrl(webServer));
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                QUERY_VALUE,
                null,
                null,
                List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                STREAM,
                new HashMap<>(),
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            var resultAsMap = result.asMap();
            assertThat(
                resultAsMap,
                is(
                    Map.of(
                        "rerank",
                        List.of(
                            Map.of("ranked_doc", Map.of("text", "candidate3", "index", 0, "relevance_score", 0.98291015625F)),
                            Map.of("ranked_doc", Map.of("text", "candidate2", "index", 2, "relevance_score", 0.61962890625F)),
                            Map.of("ranked_doc", Map.of("text", "candidate1", "index", 3, "relevance_score", 0.3642578125F))
                        )
                    )
                )
            );
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().getFirst().getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "query",
                        QUERY_VALUE,
                        "input",
                        List.of("candidate1", "candidate2", "candidate3", "candidate4"),
                        "model",
                        MODEL_NAME_VALUE,
                        "return_input",
                        RETURN_DOCUMENTS_TRUE,
                        "top_k",
                        3
                    )
                )
            );

        }
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createMixedbreadService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                        "service": "mixedbread",
                        "name": "Mixedbread",
                        "task_types": ["rerank"],
                        "configurations": {
                            "api_key": {
                                "description": "API Key for the provider you're connecting to.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
                                "type": "str",
                                "supported_task_types": ["rerank"]
                            },
                            "model_id": {
                                "description": "The model ID to use for Mixedbread requests.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["rerank"]
                            },
                            "rate_limit.requests_per_minute": {
                                "description": "Minimize the number of rate limit errors.",
                                "label": "Rate Limit",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["rerank"]
                            }
                        }
                    }
                """);
            InferenceServiceConfiguration configuration = InferenceServiceConfiguration.fromXContentBytes(
                new BytesArray(content),
                XContentType.JSON
            );
            boolean humanReadable = true;
            BytesReference originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
            InferenceServiceConfiguration serviceConfiguration = service.getConfiguration();
            assertToXContentEquivalent(
                originalBytes,
                toXContent(serviceConfiguration, XContentType.JSON, humanReadable),
                XContentType.JSON
            );
        }
    }

    private static void assertRerankModelSettings(
        Model model,
        String modelName,
        RateLimitSettings rateLimitSettings,
        String apiKey,
        MixedbreadRerankTaskSettings taskSettings
    ) {
        assertThat(model, instanceOf(MixedbreadRerankModel.class));

        var rerankModel = (MixedbreadRerankModel) model;
        assertCommonModelSettings(rerankModel, DEFAULT_RERANK_URL, modelName, rateLimitSettings, apiKey);

        assertThat(rerankModel.getTaskSettings(), is(taskSettings));
    }

    private static <T extends MixedbreadModel> void assertCommonModelSettings(
        T model,
        String url,
        String modelName,
        RateLimitSettings rateLimitSettings,
        String apiKey
    ) {
        assertThat(model.uri().toString(), is(url));
        assertThat(model.getServiceSettings().modelId(), is(modelName));
        assertThat(model.rateLimitSettings(), is(rateLimitSettings));
    }

    private MixedbreadService createMixedbreadService() {
        return new MixedbreadService(mock(HttpRequestSender.Factory.class), createWithEmptySettings(threadPool), mockClusterServiceEmpty());
    }

    @Override
    public InferenceService createInferenceService() {
        return createMixedbreadService();
    }

    @Override
    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        assertThat(rerankingInferenceService.rerankerWindowSize("any model"), is(22000));
    }
}
