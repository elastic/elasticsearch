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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceTests;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.mixedbread.embeddings.MixedbreadEmbeddingsTaskSettingsTests;
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
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
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
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class MixedbreadServiceTests extends AbstractInferenceServiceTests {
    public static final String UNKNOWN_SETTINGS_EXCEPTION =
        "Configuration contains settings [{extra_key=value}] unknown to the [mixedbread] service";

    private static final String INFERENCE_ID_VALUE = "id";
    private static final String QUERY_VALUE = "query";
    private static final Integer TOP_N = 3;
    private static final Integer REQUESTS_PER_MINUTE = 3;
    private static final Boolean STREAM = false;
    private static final List<String> INPUT = List.of("candidate1", "candidate2", "candidate3");

    private static final int DIMENSIONS_VALUE = 1536;
    private static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.COSINE;
    private static final int MAX_INPUT_TOKENS_VALUE = 512;

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    public MixedbreadServiceTests() {
        super(createTestConfiguration());
    }

    public static TestConfiguration createTestConfiguration() {
        return new TestConfiguration.Builder(new CommonConfig(TEXT_EMBEDDING, COMPLETION, EnumSet.of(TEXT_EMBEDDING, RERANK)) {

            @Override
            protected SenderService createService(ThreadPool threadPool, HttpClientManager clientManager) {
                return MixedbreadServiceTests.createService(threadPool, clientManager);
            }

            @Override
            protected Map<String, Object> createServiceSettingsMap(TaskType taskType) {
                return MixedbreadServiceTests.createServiceSettingsMap(taskType);
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
                    case TEXT_EMBEDDING -> new ModelConfigurations(
                        INFERENCE_ID_VALUE,
                        taskType,
                        MixedbreadService.NAME,
                        MixedbreadEmbeddingsServiceSettings.fromMap(
                            createServiceSettingsMap(taskType, ConfigurationParseContext.PERSISTENT),
                            ConfigurationParseContext.PERSISTENT
                        ),
                        MixedbreadEmbeddingsTaskSettings.EMPTY_SETTINGS
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
                if (taskType.equals(TEXT_EMBEDDING)) {
                    return MixedbreadEmbeddingsTaskSettingsTests.getTaskSettingsMap(null, null);
                } else if (taskType.equals(RERANK)) {
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
                assertThat(rerankingInferenceService.rerankerWindowSize(TestUtils.MODEL_ID), Matchers.is(22000));
            }
        }).enableUpdateModelTests(new UpdateModelConfiguration() {
            @Override
            protected MixedbreadEmbeddingsModel createEmbeddingModel(SimilarityMeasure similarityMeasure) {
                return createInternalEmbeddingModel(similarityMeasure);
            }
        }).build();
    }

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets) {
        switch (taskType) {
            case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets);
            case RERANK -> assertRerankModel(model, modelIncludesSecrets);
            default -> fail("unexpected task type [" + taskType + "]");
        }
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets) {
        var customModel = assertCommonModelFields(model, modelIncludesSecrets);

        assertThat(customModel.getTaskType(), Matchers.is(TaskType.TEXT_EMBEDDING));
    }

    @Override
    public void testParseRequestConfig_CreatesACompletionModel() {}

    private static void assertModel(Model model, TaskType taskType, boolean modelIncludesSecrets, ConfigurationParseContext parseContext) {
        switch (taskType) {
            case TEXT_EMBEDDING -> assertTextEmbeddingModel(model, modelIncludesSecrets, parseContext);
            case RERANK -> assertRerankModel(model, modelIncludesSecrets);
            default -> fail("unexpected task type [" + taskType + "]");
        }
    }

    private static MixedbreadEmbeddingsModel createInternalEmbeddingModel(@Nullable SimilarityMeasure similarityMeasure) {
        return new MixedbreadEmbeddingsModel(
            INFERENCE_ID_VALUE,
            new MixedbreadEmbeddingsServiceSettings(
                INFERENCE_ID_VALUE,
                TestUtils.CUSTOM_URL,
                DIMENSIONS_VALUE,
                TestUtils.ENCODING_VALUE,
                similarityMeasure,
                MAX_INPUT_TOKENS_VALUE,
                new RateLimitSettings(10_000)
            ),
            MixedbreadEmbeddingsTaskSettings.EMPTY_SETTINGS,
            createRandomChunkingSettings(),
            new DefaultSecretSettings(new SecureString(TestUtils.API_KEY.toCharArray()))
        );
    }

    private static MixedbreadModel assertCommonModelFields(Model model, boolean modelIncludesSecrets) {
        assertThat(model, instanceOf(MixedbreadModel.class));

        var mixedbreadModel = (MixedbreadModel) model;
        assertThat(mixedbreadModel.getServiceSettings().modelId(), Matchers.is(TestUtils.MODEL_ID));
        if (modelIncludesSecrets) {
            assertThat(mixedbreadModel.getSecretSettings().apiKey(), Matchers.is(new SecureString(TestUtils.API_KEY.toCharArray())));
        }
        return mixedbreadModel;
    }

    private static void assertTextEmbeddingModel(Model model, boolean modelIncludesSecrets, ConfigurationParseContext parseContext) {
        var mixedbreadModel = assertCommonModelFields(model, modelIncludesSecrets);

        assertThat(mixedbreadModel.getTaskType(), Matchers.is(TEXT_EMBEDDING));
        assertThat(model, instanceOf(MixedbreadEmbeddingsModel.class));
        var embeddingsModel = (MixedbreadEmbeddingsModel) model;
        assertThat(embeddingsModel.getTaskSettings(), Matchers.is(MixedbreadEmbeddingsTaskSettings.EMPTY_SETTINGS));
        if (parseContext.equals(ConfigurationParseContext.REQUEST)) {
            assertThat(embeddingsModel.getServiceSettings().dimensions(), Matchers.is(DIMENSIONS_VALUE));
        } else {
            assertThat(embeddingsModel.getServiceSettings().dimensions(), Matchers.is(nullValue()));
        }
        assertThat(embeddingsModel.getServiceSettings().similarity(), Matchers.is(SIMILARITY_MEASURE_VALUE));
        assertThat(embeddingsModel.getServiceSettings().maxInputTokens(), Matchers.is(MAX_INPUT_TOKENS_VALUE));
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

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType) {
        if (Objects.requireNonNull(taskType) == TEXT_EMBEDDING) {
            return MixedbreadEmbeddingsServiceSettingsTests.getServiceSettingsMap(
                TestUtils.MODEL_ID,
                TestUtils.CUSTOM_URL,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                null
            );
        }
        return MixedbreadRerankServiceSettingsTests.getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.CUSTOM_URL, null);
    }

    private static Map<String, Object> createServiceSettingsMap(TaskType taskType, ConfigurationParseContext parseContext) {
        if (Objects.requireNonNull(taskType) == TEXT_EMBEDDING) {
            if (parseContext.equals(ConfigurationParseContext.REQUEST)) {
                return MixedbreadEmbeddingsServiceSettingsTests.getServiceSettingsMap(
                    TestUtils.MODEL_ID,
                    TestUtils.CUSTOM_URL,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    null,
                    MAX_INPUT_TOKENS_VALUE,
                    null
                );
            } else {
                return MixedbreadEmbeddingsServiceSettingsTests.getServiceSettingsMap(
                    TestUtils.MODEL_ID,
                    TestUtils.CUSTOM_URL,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    null
                );
            }
        }
        return MixedbreadRerankServiceSettingsTests.getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.CUSTOM_URL, null);
    }

    private static Map<String, Object> createSecretSettingsMap() {
        return new HashMap<>(Map.of("api_key", TestUtils.API_KEY));
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
                    getServiceSettingsMap(modelName, TestUtils.DEFAULT_RERANK_URL, requestsPerMinute),
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
                getRequestConfigMap(
                    getServiceSettingsMap(modelName, TestUtils.DEFAULT_RERANK_URL, null),
                    Map.of(),
                    getSecretSettingsMap(apiKey)
                ),
                modelListener
            );

            var rerankModel = (MixedbreadRerankModel) modelListener.actionGet();

            assertThat(rerankModel.getSecretSettings().apiKey().toString(), is(apiKey));
            assertRerankModelSettings(
                modelListener.actionGet(),
                modelName,
                MixedbreadServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS,
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
                getServiceSettingsMap(modelName, TestUtils.DEFAULT_RERANK_URL, requestsPerMinute),
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

            var persistedConfig = getPersistedConfigMap(
                getServiceSettingsMap(modelName, TestUtils.DEFAULT_RERANK_URL, null),
                Map.of(),
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
                MixedbreadServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS,
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
                getServiceSettingsMap(modelName, TestUtils.DEFAULT_RERANK_URL, requestsPerMinute),
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
                    getServiceSettingsMap(null, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE),
                    getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE),
                    getSecretSettingsMap(TestUtils.API_KEY)
                ),
                modelListener
            );
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankSecretSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);
            secretSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE),
                getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE),
                secretSettings
            );

            assertThrowsExceptionWhenAnExtraKeyExists(service, config);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankServiceSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
            serviceSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                serviceSettings,
                getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE),
                getSecretSettingsMap(TestUtils.API_KEY)
            );

            assertThrowsExceptionWhenAnExtraKeyExists(service, config);
        }
    }

    public void testParseRequestConfig_ThrowsWhenAnExtraKeyExistsInRerankTaskSettingsMap() throws IOException {
        try (var service = createMixedbreadService()) {
            var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
            taskSettings.put("extra_key", "value");

            var config = getRequestConfigMap(
                getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE),
                taskSettings,
                getSecretSettingsMap(TestUtils.API_KEY)
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
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

        try (var service = createMixedbreadService()) {
            secretSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

        try (var service = createMixedbreadService()) {
            serviceSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfig_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

        try (var service = createMixedbreadService()) {
            taskSettings.put("extra_key", "value");

            var persistedConfig = getPersistedConfigMap(serviceSettings, taskSettings, secretSettings);

            var model = service.parsePersistedConfig(INFERENCE_ID_VALUE, RERANK, persistedConfig.config());

            assertThat(model, CoreMatchers.instanceOf(MixedbreadRerankModel.class));

            var rerankModel = (MixedbreadRerankModel) model;
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInSecretsSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

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
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(TestUtils.API_KEY));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInServiceSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

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
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(TestUtils.API_KEY));
        }
    }

    public void testParsePersistedConfigWithSecrets_DoesNotThrowWhenAnExtraKeyExistsInTaskSettings() throws IOException {
        var serviceSettings = getServiceSettingsMap(TestUtils.MODEL_ID, TestUtils.DEFAULT_RERANK_URL, REQUESTS_PER_MINUTE);
        var taskSettings = getTaskSettingsMap(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE);
        var secretSettings = getSecretSettingsMap(TestUtils.API_KEY);

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
            assertThat(rerankModel.getServiceSettings().modelId(), is(TestUtils.MODEL_ID));
            assertThat(rerankModel.getTaskSettings(), is(new MixedbreadRerankTaskSettings(TOP_N, TestUtils.RETURN_DOCUMENTS_TRUE)));
            assertThat(rerankModel.getSecretSettings().apiKey(), is(TestUtils.API_KEY));
        }
    }

    public void testInfer_Rerank_UnauthorisedResponse() throws IOException {
        var model = MixedbreadRerankModelTests.createModel(
            TestUtils.MODEL_ID,
            TestUtils.API_KEY,
            TOP_N,
            TestUtils.RETURN_DOCUMENTS_FALSE,
            getUrl(webServer)
        );

        assertUnauthorisedResponse(model);
    }

    public void testInfer_TextEmbedding_UnauthorisedResponse() throws IOException {
        var model = MixedbreadEmbeddingsModelTests.createModel(
            getUrl(webServer),
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            null,
            null,
            null,
            null,
            null
        );

        assertUnauthorisedResponse(model);
    }

    private void assertUnauthorisedResponse(MixedbreadModel model) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var service = new MixedbreadService(senderFactory, createWithEmptySettings(threadPool), mockClusterServiceEmpty())) {

            String responseJson = """
                {
                    "detail": "Unauthorized"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(401).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            service.infer(
                model,
                QUERY_VALUE,
                null,
                null,
                List.of("abc"),
                false,
                new HashMap<>(),
                InputType.INGEST,
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
            var model = MixedbreadRerankModelTests.createModel(
                TestUtils.MODEL_ID,
                TestUtils.API_KEY,
                null,
                TestUtils.RETURN_DOCUMENTS_FALSE,
                getUrl(webServer)
            );
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
                is(
                    Map.of(
                        "query",
                        QUERY_VALUE,
                        "input",
                        INPUT,
                        "model",
                        TestUtils.MODEL_ID,
                        "return_input",
                        TestUtils.RETURN_DOCUMENTS_FALSE
                    )
                )
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
            var model = MixedbreadRerankModelTests.createModel(TestUtils.MODEL_ID, TestUtils.API_KEY, null, null, getUrl(webServer));
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
            assertThat(requestMap, is(Map.of("query", QUERY_VALUE, "input", INPUT, "model", TestUtils.MODEL_ID)));

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
            var model = MixedbreadRerankModelTests.createModel(
                TestUtils.MODEL_ID,
                TestUtils.API_KEY,
                TOP_N,
                TestUtils.RETURN_DOCUMENTS_TRUE,
                getUrl(webServer)
            );
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
                        TestUtils.MODEL_ID,
                        "return_input",
                        TestUtils.RETURN_DOCUMENTS_TRUE,
                        "top_k",
                        3
                    )
                )
            );

        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsProvided() throws IOException {
        var chunkingSettings = createRandomChunkingSettings();
        try (var service = createMixedbreadService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(MixedbreadEmbeddingsModel.class));

                var embeddingsModel = (MixedbreadEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), Matchers.is(TestUtils.CUSTOM_URL));
                assertThat(embeddingsModel.getServiceSettings().modelId(), Matchers.is(TestUtils.MODEL_ID));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
                assertThat(embeddingsModel.getConfigurations().getChunkingSettings().asMap(), Matchers.is(chunkingSettings.asMap()));
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), Matchers.is(TestUtils.API_KEY));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TEXT_EMBEDDING,
                getRequestConfigWithChunkedSettingsMap(
                    createServiceSettingsMap(TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                    chunkingSettings.asMap(),
                    getSecretSettingsMap(TestUtils.API_KEY)
                ),
                modelVerificationActionListener
            );
        }
    }

    public void testParseRequestConfig_CreatesAnEmbeddingsModelWhenChunkingSettingsNotProvided() throws IOException {
        try (var service = createMixedbreadService()) {
            ActionListener<Model> modelVerificationActionListener = ActionListener.wrap(model -> {
                assertThat(model, instanceOf(MixedbreadEmbeddingsModel.class));

                var embeddingsModel = (MixedbreadEmbeddingsModel) model;
                assertThat(embeddingsModel.getServiceSettings().uri().toString(), Matchers.is(TestUtils.CUSTOM_URL));
                assertThat(embeddingsModel.getServiceSettings().modelId(), Matchers.is(TestUtils.MODEL_ID));
                assertThat(
                    embeddingsModel.getConfigurations().getChunkingSettings(),
                    Matchers.is(ChunkingSettingsBuilder.DEFAULT_SETTINGS)
                );
                assertThat(embeddingsModel.getSecretSettings().apiKey().toString(), Matchers.is(TestUtils.API_KEY));
            }, e -> fail("parse request should not fail " + e.getMessage()));

            service.parseRequestConfig(
                INFERENCE_ID_VALUE,
                TEXT_EMBEDDING,
                getRequestConfigMap(
                    createServiceSettingsMap(TEXT_EMBEDDING, ConfigurationParseContext.REQUEST),
                    Map.of(),
                    getSecretSettingsMap(TestUtils.API_KEY)
                ),
                modelVerificationActionListener
            );
        }
    }

    private Map<String, Object> getRequestConfigWithChunkedSettingsMap(
        Map<String, Object> serviceSettings,
        Map<String, Object> chunkingSettings,
        Map<String, Object> secretSettings
    ) {
        var requestConfigMap = getRequestConfigMap(serviceSettings, Map.of(), secretSettings);

        requestConfigMap.put(ModelConfigurations.CHUNKING_SETTINGS, chunkingSettings);

        return requestConfigMap;
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createMixedbreadService()) {
            String content = XContentHelper.stripWhitespace("""
                {
                        "service": "mixedbread",
                        "name": "Mixedbread",
                        "task_types": ["text_embedding", "rerank"],
                        "configurations": {
                            "dimensions": {
                                "description":"The number of dimensions the resulting embeddings should have",
                                "label": "Dimensions",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["text_embedding"]
                            },
                            "api_key": {
                                "description": "API Key for the provider you're connecting to.",
                                "label": "API Key",
                                "required": true,
                                "sensitive": true,
                                "updatable": true,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank"]
                            },
                            "model_id": {
                                "description": "The model ID to use for Mixedbread requests.",
                                "label": "Model ID",
                                "required": true,
                                "sensitive": false,
                                "updatable": false,
                                "type": "str",
                                "supported_task_types": ["text_embedding", "rerank"]
                            },
                            "rate_limit.requests_per_minute": {
                                "description": "Minimize the number of rate limit errors.",
                                "label": "Rate Limit",
                                "required": false,
                                "sensitive": false,
                                "updatable": false,
                                "type": "int",
                                "supported_task_types": ["text_embedding", "rerank"]
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

        assertThat(rerankModel.getServiceSettings().uri().toString(), is(TestUtils.DEFAULT_RERANK_URL));
        assertThat(rerankModel.getServiceSettings().modelId(), is(modelName));
        assertThat(rerankModel.rateLimitSettings(), is(rateLimitSettings));

        assertThat(rerankModel.getTaskSettings(), is(taskSettings));
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
