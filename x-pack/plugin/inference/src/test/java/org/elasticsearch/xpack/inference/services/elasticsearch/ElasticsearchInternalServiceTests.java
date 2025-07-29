/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceError;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResultsTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.ModelConfigurationsTests;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkingSettings;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response.RESULTS_FIELD;
import static org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettingsMap;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.NAME;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticsearchInternalServiceTests extends ESTestCase {

    String randomInferenceEntityId = randomAlphaOfLength(10);

    private static ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = createThreadPool(InferencePlugin.inferenceUtilityExecutor(Settings.EMPTY));
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testParseRequestConfig() {
        // Null model variant
        var service = createService(mock(Client.class));
        var config = new HashMap<String, Object>();
        config.put(ModelConfigurations.SERVICE, ElasticsearchInternalService.NAME);
        config.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
            )
        );

        ActionListener<Model> modelListener = ActionListener.<Model>wrap(
            model -> fail("Model parsing should have failed"),
            e -> assertThat(e, instanceOf(IllegalArgumentException.class))
        );

        var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING);
        service.parseRequestConfig(randomInferenceEntityId, taskType, config, modelListener);
    }

    public void testParseRequestConfig_Misconfigured() {
        // Non-existent model variant
        {
            var service = createService(mock(Client.class));
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, ElasticsearchInternalService.NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(IllegalArgumentException.class))
            );

            var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING);
            service.parseRequestConfig(randomInferenceEntityId, taskType, config, modelListener);
        }

        // Invalid config map
        {
            var service = createService(mock(Client.class));
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, ElasticsearchInternalService.NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
                )
            );
            config.put("not_a_valid_config_setting", randomAlphaOfLength(10));

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING);
            service.parseRequestConfig(randomInferenceEntityId, taskType, config, modelListener);
        }
    }

    public void testParseRequestConfig_E5() {
        {
            var service = createService(mock(Client.class), BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(1, 4, MULTILINGUAL_E5_SMALL_MODEL_ID, null);

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings,
                getE5ModelVerificationActionListener(e5ServiceSettings, false)
            );
        }

        {
            var service = createService(mock(Client.class), BaseElasticsearchInternalService.PreferredModelVariant.LINUX_X86_OPTIMIZED);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86,
                null
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings,
                getE5ModelVerificationActionListener(e5ServiceSettings, false)
            );
        }

        // Invalid service settings
        {
            var service = createService(mock(Client.class), BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        MULTILINGUAL_E5_SMALL_MODEL_ID,
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, TaskType.TEXT_EMBEDDING, settings, modelListener);
        }

        {
            var service = createService(mock(Client.class), BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );
            settings.put(ModelConfigurations.CHUNKING_SETTINGS, createRandomChunkingSettingsMap());

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(1, 4, MULTILINGUAL_E5_SMALL_MODEL_ID, null);

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings,
                getE5ModelVerificationActionListener(e5ServiceSettings, true)
            );
        }

        {
            var service = createService(mock(Client.class), BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(1, 4, MULTILINGUAL_E5_SMALL_MODEL_ID, null);

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings,
                getE5ModelVerificationActionListener(e5ServiceSettings, true)
            );
        }
    }

    public void testParseRequestConfig_elser() {
        // General happy case
        {
            Client mockClient = mock(Client.class);
            when(mockClient.threadPool()).thenReturn(threadPool);
            var service = createService(mockClient);
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, OLD_ELSER_SERVICE_NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElserModels.ELSER_V2_MODEL
                    )
                )
            );

            var elserServiceSettings = new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(1, 4, ElserModels.ELSER_V2_MODEL, null, null)
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.SPARSE_EMBEDDING,
                config,
                getElserModelVerificationActionListener(
                    elserServiceSettings,
                    null,
                    "The [elser] service is deprecated and will be removed in a future release. Use the [elasticsearch] service "
                        + "instead, with [model_id] set to [.elser_model_2] in the [service_settings]",
                    false
                )
            );
        }

        // null model ID returns elser model for the provided platform (not linux)
        {
            Client mockClient = mock(Client.class);
            when(mockClient.threadPool()).thenReturn(threadPool);
            var service = createService(mockClient);
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, OLD_ELSER_SERVICE_NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
                )
            );

            var elserServiceSettings = new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(1, 4, ElserModels.ELSER_V2_MODEL, null, null)
            );

            String criticalWarning =
                "Putting elasticsearch service inference endpoints (including elser service) without a model_id field is"
                    + " deprecated and will be removed in a future release. Please specify a model_id field.";
            String warnWarning =
                "The [elser] service is deprecated and will be removed in a future release. Use the [elasticsearch] service "
                    + "instead, with [model_id] set to [.elser_model_2] in the [service_settings]";
            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.SPARSE_EMBEDDING,
                config,
                getElserModelVerificationActionListener(elserServiceSettings, criticalWarning, warnWarning, false)
            );
            assertWarnings(true, new DeprecationWarning(DeprecationLogger.CRITICAL, criticalWarning));
        }

        // Invalid service settings
        {
            Client mockClient = mock(Client.class);
            when(mockClient.threadPool()).thenReturn(threadPool);
            var service = createService(mockClient);
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, OLD_ELSER_SERVICE_NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElserModels.ELSER_V2_MODEL,
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, TaskType.SPARSE_EMBEDDING, config, modelListener);
        }

        {
            Client mockClient = mock(Client.class);
            when(mockClient.threadPool()).thenReturn(threadPool);
            var service = createService(mockClient);
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, OLD_ELSER_SERVICE_NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElserModels.ELSER_V2_MODEL
                    )
                )
            );
            config.put(ModelConfigurations.CHUNKING_SETTINGS, createRandomChunkingSettingsMap());

            var elserServiceSettings = new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(1, 4, ElserModels.ELSER_V2_MODEL, null, null)
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.SPARSE_EMBEDDING,
                config,
                getElserModelVerificationActionListener(
                    elserServiceSettings,
                    null,
                    "The [elser] service is deprecated and will be removed in a future release. Use the [elasticsearch] service "
                        + "instead, with [model_id] set to [.elser_model_2] in the [service_settings]",
                    true
                )
            );
        }

        {
            Client mockClient = mock(Client.class);
            when(mockClient.threadPool()).thenReturn(threadPool);
            var service = createService(mockClient);
            var config = new HashMap<String, Object>();
            config.put(ModelConfigurations.SERVICE, OLD_ELSER_SERVICE_NAME);
            config.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElserModels.ELSER_V2_MODEL
                    )
                )
            );

            var elserServiceSettings = new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(1, 4, ElserModels.ELSER_V2_MODEL, null, null)
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                TaskType.SPARSE_EMBEDDING,
                config,
                getElserModelVerificationActionListener(
                    elserServiceSettings,
                    null,
                    "The [elser] service is deprecated and will be removed in a future release. Use the [elasticsearch] service "
                        + "instead, with [model_id] set to [.elser_model_2] in the [service_settings]",
                    true
                )
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testParseRequestConfig_Rerank() {
        // with task settings
        {
            var client = mock(Client.class);
            doAnswer(invocation -> {
                var listener = (ActionListener<GetTrainedModelsAction.Response>) invocation.getArguments()[2];
                var modelConfig = mock(TrainedModelConfig.class);
                when(modelConfig.getInferenceConfig()).thenReturn(mock(TextSimilarityConfig.class));
                listener.onResponse(new GetTrainedModelsAction.Response(new QueryPage<>(List.of(modelConfig), 1, mock(ParseField.class))));
                return null;
            }).when(client).execute(Mockito.same(GetTrainedModelsAction.INSTANCE), any(), any());

            when(client.threadPool()).thenReturn(threadPool);

            var service = createService(client);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        "foo"
                    )
                )
            );
            var returnDocs = randomBoolean();
            settings.put(ModelConfigurations.TASK_SETTINGS, new HashMap<>(Map.of(RerankTaskSettings.RETURN_DOCUMENTS, returnDocs)));

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(model -> {
                assertThat(model, instanceOf(CustomElandRerankModel.class));
                assertThat(model.getTaskSettings(), instanceOf(RerankTaskSettings.class));
                assertThat(model.getServiceSettings(), instanceOf(CustomElandInternalServiceSettings.class));
                assertEquals(returnDocs, ((RerankTaskSettings) model.getTaskSettings()).returnDocuments());
            }, e -> { fail("Model parsing failed " + e.getMessage()); });

            service.parseRequestConfig(randomInferenceEntityId, TaskType.RERANK, settings, modelListener);
        }
    }

    @SuppressWarnings("unchecked")
    public void testParseRequestConfig_Rerank_DefaultTaskSettings() {
        // with task settings
        {
            var client = mock(Client.class);
            doAnswer(invocation -> {
                var listener = (ActionListener<GetTrainedModelsAction.Response>) invocation.getArguments()[2];
                var modelConfig = mock(TrainedModelConfig.class);
                when(modelConfig.getInferenceConfig()).thenReturn(mock(TextSimilarityConfig.class));
                listener.onResponse(new GetTrainedModelsAction.Response(new QueryPage<>(List.of(modelConfig), 1, mock(ParseField.class))));
                return null;
            }).when(client).execute(Mockito.same(GetTrainedModelsAction.INSTANCE), any(), any());

            when(client.threadPool()).thenReturn(threadPool);

            var service = createService(client);
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        "foo"
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(model -> {
                assertThat(model, instanceOf(CustomElandRerankModel.class));
                assertThat(model.getTaskSettings(), instanceOf(RerankTaskSettings.class));
                assertThat(model.getServiceSettings(), instanceOf(CustomElandInternalServiceSettings.class));
                assertEquals(Boolean.TRUE, ((RerankTaskSettings) model.getTaskSettings()).returnDocuments());
            }, e -> { fail("Model parsing failed " + e.getMessage()); });

            service.parseRequestConfig(randomInferenceEntityId, TaskType.RERANK, settings, modelListener);
        }
    }

    public void testParseRequestConfig_SparseEmbeddingWithoutChunkingSettings() {
        testParseRequestConfig_SparseEmbedding(false, Optional.empty());
    }

    public void testParseRequestConfig_SparseEmbeddingWithChunkingSettingsProvided() {
        testParseRequestConfig_SparseEmbedding(true, Optional.of(createRandomChunkingSettingsMap()));
    }

    public void testParseRequestConfig_SparseEmbeddingWithChunkingSettingsNotProvided() {
        testParseRequestConfig_SparseEmbedding(true, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private void testParseRequestConfig_SparseEmbedding(
        boolean validateChunkingSettings,
        Optional<Map<String, Object>> chunkingSettingsMap
    ) {
        var client = mock(Client.class);
        doAnswer(invocation -> {
            var listener = (ActionListener<GetTrainedModelsAction.Response>) invocation.getArguments()[2];
            var modelConfig = mock(TrainedModelConfig.class);
            when(modelConfig.getInferenceConfig()).thenReturn(mock(TextExpansionConfig.class));
            listener.onResponse(new GetTrainedModelsAction.Response(new QueryPage<>(List.of(modelConfig), 1, mock(ParseField.class))));
            return null;
        }).when(client).execute(Mockito.same(GetTrainedModelsAction.INSTANCE), any(), any());

        when(client.threadPool()).thenReturn(threadPool);

        var service = createService(client);
        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(
                    ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElasticsearchInternalServiceSettings.NUM_THREADS,
                    4,
                    ElasticsearchInternalServiceSettings.MODEL_ID,
                    "foo"
                )
            )
        );
        chunkingSettingsMap.ifPresent(stringObjectMap -> settings.put(ModelConfigurations.CHUNKING_SETTINGS, stringObjectMap));

        ActionListener<Model> modelListener = ActionListener.<Model>wrap(model -> {
            assertThat(model, instanceOf(CustomElandModel.class));
            assertThat(model.getTaskSettings(), instanceOf(EmptyTaskSettings.class));
            assertThat(model.getServiceSettings(), instanceOf(CustomElandInternalServiceSettings.class));
            if (validateChunkingSettings) {
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            }
        }, e -> { fail("Model parsing failed " + e.getMessage()); });

        service.parseRequestConfig(randomInferenceEntityId, TaskType.SPARSE_EMBEDDING, settings, modelListener);
    }

    private ActionListener<Model> getE5ModelVerificationActionListener(
        MultilingualE5SmallInternalServiceSettings e5ServiceSettings,
        boolean expectChunkingSettings
    ) {
        return ActionListener.<Model>wrap(model -> {
            assertThat(model, instanceOf(MultilingualE5SmallModel.class));
            MultilingualE5SmallModel multilingualE5SmallModel = (MultilingualE5SmallModel) model;

            assertEquals(randomInferenceEntityId, multilingualE5SmallModel.getInferenceEntityId());
            assertEquals(TaskType.TEXT_EMBEDDING, multilingualE5SmallModel.getTaskType());
            assertEquals(ElasticsearchInternalService.NAME, multilingualE5SmallModel.getConfigurations().getService());
            assertEquals(e5ServiceSettings, multilingualE5SmallModel.getServiceSettings());
            if (expectChunkingSettings) {
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            }
        }, e -> { fail("Model parsing failed " + e.getMessage()); });
    }

    private ActionListener<Model> getElserModelVerificationActionListener(
        ElserInternalServiceSettings elserServiceSettings,
        String criticalWarning,
        String warnWarning,
        boolean expectChunkingSettings
    ) {
        return ActionListener.wrap(model -> {
            assertWarnings(
                true,
                new DeprecationWarning(DeprecationLogger.CRITICAL, criticalWarning),
                new DeprecationWarning(Level.WARN, warnWarning)
            );

            assertThat(model, instanceOf(ElserInternalModel.class));
            ElserInternalModel elserInternalModel = (ElserInternalModel) model;
            assertEquals(randomInferenceEntityId, elserInternalModel.getInferenceEntityId());
            assertEquals(TaskType.SPARSE_EMBEDDING, elserInternalModel.getTaskType());
            assertEquals(NAME, elserInternalModel.getConfigurations().getService());
            assertEquals(elserServiceSettings, elserInternalModel.getServiceSettings());
            if (expectChunkingSettings) {
                assertThat(model.getConfigurations().getChunkingSettings(), instanceOf(ChunkingSettings.class));
            }

        }, e -> { fail("Model parsing failed " + e.getMessage()); });
    }

    public void testParsePersistedConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ServiceFields.SIMILARITY,
                        SimilarityMeasure.L2_NORM.toString()
                    )
                )
            );

            expectThrows(
                IllegalArgumentException.class,
                () -> service.parsePersistedConfig(randomInferenceEntityId, TaskType.TEXT_EMBEDDING, settings)
            );

        }

        // Invalid model variant
        // because this is a persisted config, we assume that the model does exist, even though it doesn't. In practice, the trained models
        // API would throw an exception when the model is used
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        "invalid"
                    )
                )
            );

            CustomElandEmbeddingModel parsedModel = (CustomElandEmbeddingModel) service.parsePersistedConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings
            );
            var elandServiceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
                1,
                4,
                "invalid",
                null,
                null,
                null,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            );
            assertEquals(
                new CustomElandEmbeddingModel(
                    randomInferenceEntityId,
                    TaskType.TEXT_EMBEDDING,
                    ElasticsearchInternalService.NAME,
                    elandServiceSettings,
                    null
                ),
                parsedModel
            );
        }

        // Valid model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        MULTILINGUAL_E5_SMALL_MODEL_ID,
                        ServiceFields.DIMENSIONS,
                        1
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(1, 4, MULTILINGUAL_E5_SMALL_MODEL_ID, null);

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parsePersistedConfig(
                randomInferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                settings
            );
            assertEquals(
                new MultilingualE5SmallModel(
                    randomInferenceEntityId,
                    TaskType.TEXT_EMBEDDING,
                    ElasticsearchInternalService.NAME,
                    e5ServiceSettings,
                    null
                ),
                parsedModel
            );
        }

        // Invalid config map
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
                )
            );
            settings.put("not_a_valid_config_setting", randomAlphaOfLength(10));

            var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING);
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }

        // Invalid service settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );
            var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.RERANK, TaskType.SPARSE_EMBEDDING);
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided() {
        var service = createService(mock(Client.class));
        var model = new Model(ModelConfigurationsTests.createRandomInstance());

        assertThrows(ElasticsearchStatusException.class, () -> { service.updateModelWithEmbeddingDetails(model, randomNonNegativeInt()); });
    }

    public void testUpdateModelWithEmbeddingDetails_TextEmbeddingCustomElandEmbeddingsModelUpdatesDimensions() {
        var service = createService(mock(Client.class));
        var elandServiceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
            1,
            4,
            "invalid",
            null,
            null,
            null,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        var model = new CustomElandEmbeddingModel(
            randomAlphaOfLength(10),
            TaskType.TEXT_EMBEDDING,
            "elasticsearch",
            elandServiceSettings,
            null
        );

        var embeddingSize = randomNonNegativeInt();
        var updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

        assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
    }

    public void testUpdateModelWithEmbeddingDetails_NonTextEmbeddingCustomElandEmbeddingsModelNotModified() {
        var service = createService(mock(Client.class));
        var elandServiceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
            1,
            4,
            "invalid",
            null,
            null,
            null,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        var model = new CustomElandEmbeddingModel(
            randomAlphaOfLength(10),
            TaskType.SPARSE_EMBEDDING,
            "elasticsearch",
            elandServiceSettings,
            null
        );

        var embeddingSize = randomNonNegativeInt();
        var updatedModel = service.updateModelWithEmbeddingDetails(model, embeddingSize);

        assertEquals(model, updatedModel);
    }

    public void testUpdateModelWithEmbeddingDetails_ElasticsearchInternalModelNotModified() {
        var service = createService(mock(Client.class));
        var model = mock(ElasticsearchInternalModel.class);

        var updatedModel = service.updateModelWithEmbeddingDetails(model, randomNonNegativeInt());

        assertEquals(model, updatedModel);
        verifyNoMoreInteractions(model);
    }

    public void testChunkInfer_E5WithNullChunkingSettings() throws InterruptedException {
        testChunkInfer_e5(null);
    }

    public void testChunkInfer_E5ChunkingSettingsSet() throws InterruptedException {
        testChunkInfer_e5(ChunkingSettingsTests.createRandomChunkingSettings());
    }

    @SuppressWarnings("unchecked")
    private void testChunkInfer_e5(ChunkingSettings chunkingSettings) throws InterruptedException {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(MlTextEmbeddingResultsTests.createRandomResults());
        mlTrainedModelResults.add(MlTextEmbeddingResultsTests.createRandomResults());
        var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var model = new MultilingualE5SmallModel(
            "foo",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null),
            chunkingSettings
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInference>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(2));
            assertThat(chunkedResponse.get(0), instanceOf(ChunkedInferenceEmbedding.class));
            var result1 = (ChunkedInferenceEmbedding) chunkedResponse.get(0);
            assertThat(result1.chunks(), hasSize(1));
            assertThat(result1.chunks().get(0).embedding(), instanceOf(TextEmbeddingFloatResults.Embedding.class));
            assertArrayEquals(
                ((MlTextEmbeddingResults) mlTrainedModelResults.get(0)).getInferenceAsFloat(),
                ((TextEmbeddingFloatResults.Embedding) result1.chunks().get(0).embedding()).values(),
                0.0001f
            );
            assertEquals(new ChunkedInference.TextOffset(0, 1), result1.chunks().get(0).offset());
            assertThat(chunkedResponse.get(1), instanceOf(ChunkedInferenceEmbedding.class));
            var result2 = (ChunkedInferenceEmbedding) chunkedResponse.get(1);
            assertThat(result2.chunks(), hasSize(1));
            assertThat(result2.chunks().get(0).embedding(), instanceOf(TextEmbeddingFloatResults.Embedding.class));
            assertArrayEquals(
                ((MlTextEmbeddingResults) mlTrainedModelResults.get(1)).getInferenceAsFloat(),
                ((TextEmbeddingFloatResults.Embedding) result2.chunks().get(0).embedding()).values(),
                0.0001f
            );
            assertEquals(new ChunkedInference.TextOffset(0, 2), result2.chunks().get(0).offset());

            gotResults.set(true);
        }, ESTestCase::fail);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(resultsListener, latch);

        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            latchedListener
        );

        latch.await();
        assertTrue("Listener not called", gotResults.get());
    }

    public void testChunkInfer_SparseWithNullChunkingSettings() throws InterruptedException {
        testChunkInfer_Sparse(null);
    }

    public void testChunkInfer_SparseWithChunkingSettingsSet() throws InterruptedException {
        testChunkInfer_Sparse(ChunkingSettingsTests.createRandomChunkingSettings());
    }

    @SuppressWarnings("unchecked")
    private void testChunkInfer_Sparse(ChunkingSettings chunkingSettings) throws InterruptedException {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(TextExpansionResultsTests.createRandomResults());
        mlTrainedModelResults.add(TextExpansionResultsTests.createRandomResults());
        var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var model = new CustomElandModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            "elasticsearch",
            new ElasticsearchInternalServiceSettings(1, 1, "model-id", null, null),
            chunkingSettings
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();

        var resultsListener = ActionListener.<List<ChunkedInference>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(2));
            assertThat(chunkedResponse.get(0), instanceOf(ChunkedInferenceEmbedding.class));
            var result1 = (ChunkedInferenceEmbedding) chunkedResponse.get(0);
            assertThat(result1.chunks().get(0).embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
            assertEquals(
                ((TextExpansionResults) mlTrainedModelResults.get(0)).getWeightedTokens(),
                ((SparseEmbeddingResults.Embedding) result1.chunks().get(0).embedding()).tokens()
            );
            assertEquals(new ChunkedInference.TextOffset(0, 1), result1.chunks().get(0).offset());
            assertThat(chunkedResponse.get(1), instanceOf(ChunkedInferenceEmbedding.class));
            var result2 = (ChunkedInferenceEmbedding) chunkedResponse.get(1);
            assertThat(result2.chunks().get(0).embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
            assertEquals(
                ((TextExpansionResults) mlTrainedModelResults.get(1)).getWeightedTokens(),
                ((SparseEmbeddingResults.Embedding) result2.chunks().get(0).embedding()).tokens()
            );
            assertEquals(new ChunkedInference.TextOffset(0, 2), result2.chunks().get(0).offset());
            gotResults.set(true);
        }, ESTestCase::fail);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(resultsListener, latch);

        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            latchedListener
        );

        latch.await();
        assertTrue("Listener not called", gotResults.get());
    }

    public void testChunkInfer_ElserWithNullChunkingSettings() throws InterruptedException {
        testChunkInfer_Elser(null);
    }

    public void testChunkInfer_ElserWithChunkingSettingsSet() throws InterruptedException {
        testChunkInfer_Elser(ChunkingSettingsTests.createRandomChunkingSettings());
    }

    @SuppressWarnings("unchecked")
    private void testChunkInfer_Elser(ChunkingSettings chunkingSettings) throws InterruptedException {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(TextExpansionResultsTests.createRandomResults());
        mlTrainedModelResults.add(TextExpansionResultsTests.createRandomResults());
        var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var model = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            "elasticsearch",
            new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 1, "model-id", null, null)),
            new ElserMlNodeTaskSettings(),
            chunkingSettings
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInference>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(2));
            assertThat(chunkedResponse.get(0), instanceOf(ChunkedInferenceEmbedding.class));
            var result1 = (ChunkedInferenceEmbedding) chunkedResponse.get(0);
            assertThat(result1.chunks().get(0).embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
            assertEquals(
                ((TextExpansionResults) mlTrainedModelResults.get(0)).getWeightedTokens(),
                ((SparseEmbeddingResults.Embedding) result1.chunks().get(0).embedding()).tokens()
            );
            assertEquals(new ChunkedInference.TextOffset(0, 1), result1.chunks().get(0).offset());
            assertThat(chunkedResponse.get(1), instanceOf(ChunkedInferenceEmbedding.class));
            var result2 = (ChunkedInferenceEmbedding) chunkedResponse.get(1);
            assertThat(result2.chunks().get(0).embedding(), instanceOf(SparseEmbeddingResults.Embedding.class));
            assertEquals(
                ((TextExpansionResults) mlTrainedModelResults.get(1)).getWeightedTokens(),
                ((SparseEmbeddingResults.Embedding) result2.chunks().get(0).embedding()).tokens()
            );
            assertEquals(new ChunkedInference.TextOffset(0, 2), result2.chunks().get(0).offset());
            gotResults.set(true);
        }, ESTestCase::fail);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(resultsListener, latch);

        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("a"), new ChunkInferenceInput("bb")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            latchedListener
        );

        latch.await();
        assertTrue("Listener not called", gotResults.get());
    }

    @SuppressWarnings("unchecked")
    public void testChunkInferSetsTokenization() {
        var expectedSpan = new AtomicInteger();
        var expectedWindowSize = new AtomicReference<Integer>();

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            var request = (InferTrainedModelDeploymentAction.Request) invocationOnMock.getArguments()[1];
            assertThat(request.getUpdate(), instanceOf(TokenizationConfigUpdate.class));
            var update = (TokenizationConfigUpdate) request.getUpdate();
            assertEquals(update.getSpanSettings().span(), expectedSpan.get());
            assertEquals(update.getSpanSettings().maxSequenceLength(), expectedWindowSize.get());
            return null;
        }).when(client)
            .execute(
                same(InferTrainedModelDeploymentAction.INSTANCE),
                any(InferTrainedModelDeploymentAction.Request.class),
                any(ActionListener.class)
            );

        var model = new MultilingualE5SmallModel(
            "foo",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null),
            null
        );
        var service = createService(client);

        expectedSpan.set(-1);
        expectedWindowSize.set(null);
        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("foo"), new ChunkInferenceInput("bar")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            ActionListener.wrap(r -> fail("unexpected result"), e -> fail(e.getMessage()))
        );

        expectedSpan.set(-1);
        expectedWindowSize.set(256);
        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("foo"), new ChunkInferenceInput("bar")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            ActionListener.wrap(r -> fail("unexpected result"), e -> fail(e.getMessage()))
        );

    }

    @SuppressWarnings("unchecked")
    public void testChunkInfer_FailsBatch() throws InterruptedException {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(MlTextEmbeddingResultsTests.createRandomResults());
        mlTrainedModelResults.add(MlTextEmbeddingResultsTests.createRandomResults());
        mlTrainedModelResults.add(new ErrorInferenceResults(new RuntimeException("boom")));
        var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var model = new MultilingualE5SmallModel(
            "foo",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null),
            null
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInference>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(3));
            // a single failure fails the batch
            for (var er : chunkedResponse) {
                assertThat(er, instanceOf(ChunkedInferenceError.class));
                assertEquals("boom", ((ChunkedInferenceError) er).exception().getMessage());
            }

            gotResults.set(true);
        }, ESTestCase::fail);

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(resultsListener, latch);

        service.chunkedInfer(
            model,
            null,
            List.of(new ChunkInferenceInput("foo"), new ChunkInferenceInput("bar"), new ChunkInferenceInput("baz")),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            latchedListener
        );

        latch.await();
        assertTrue("Listener not called", gotResults.get());
    }

    @SuppressWarnings("unchecked")
    public void testChunkingLargeDocument() throws InterruptedException {
        int numBatches = randomIntBetween(3, 6);

        // how many response objects to return in each batch
        int[] numResponsesPerBatch = new int[numBatches];
        for (int i = 0; i < numBatches - 1; i++) {
            numResponsesPerBatch[i] = ElasticsearchInternalService.EMBEDDING_MAX_BATCH_SIZE;
        }
        numResponsesPerBatch[numBatches - 1] = randomIntBetween(1, ElasticsearchInternalService.EMBEDDING_MAX_BATCH_SIZE);
        int numChunks = Arrays.stream(numResponsesPerBatch).sum();

        // build a doc with enough words to make numChunks of chunks
        int wordsPerChunk = 10;
        int numWords = numChunks * wordsPerChunk;
        var input = new ChunkInferenceInput("word ".repeat(numWords), null);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        // mock the inference response
        doAnswer(invocationOnMock -> {
            var request = (InferModelAction.Request) invocationOnMock.getArguments()[1];
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            var mlTrainedModelResults = new ArrayList<InferenceResults>();
            for (int i = 0; i < request.numberOfDocuments(); i++) {
                mlTrainedModelResults.add(MlTextEmbeddingResultsTests.createRandomResults());
            }
            var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInference>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(1));
            assertThat(chunkedResponse.get(0), instanceOf(ChunkedInferenceEmbedding.class));
            var sparseResults = (ChunkedInferenceEmbedding) chunkedResponse.get(0);
            assertThat(sparseResults.chunks(), hasSize(numChunks));

            gotResults.set(true);
        }, ESTestCase::fail);

        // Create model using the word boundary chunker.
        var model = new MultilingualE5SmallModel(
            "foo",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null),
            new WordBoundaryChunkingSettings(wordsPerChunk, 0)
        );

        var latch = new CountDownLatch(1);
        var latchedListener = new LatchedActionListener<>(resultsListener, latch);

        // For the given input we know how many requests will be made
        service.chunkedInfer(
            model,
            null,
            List.of(input),
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            latchedListener
        );

        latch.await();
        assertTrue("Listener not called with results", gotResults.get());
    }

    public void testParsePersistedConfig_Rerank() {
        // with task settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        "foo"
                    )
                )
            );
            settings.put(ElasticsearchInternalServiceSettings.MODEL_ID, "foo");
            var returnDocs = randomBoolean();
            settings.put(ModelConfigurations.TASK_SETTINGS, new HashMap<>(Map.of(RerankTaskSettings.RETURN_DOCUMENTS, returnDocs)));

            var model = service.parsePersistedConfig(randomInferenceEntityId, TaskType.RERANK, settings);
            assertThat(model.getTaskSettings(), instanceOf(RerankTaskSettings.class));
            assertEquals(returnDocs, ((RerankTaskSettings) model.getTaskSettings()).returnDocuments());
        }

        // without task settings
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(
                        ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElasticsearchInternalServiceSettings.NUM_THREADS,
                        4,
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        "foo"
                    )
                )
            );
            settings.put(ElasticsearchInternalServiceSettings.MODEL_ID, "foo");

            var model = service.parsePersistedConfig(randomInferenceEntityId, TaskType.RERANK, settings);
            assertThat(model.getTaskSettings(), instanceOf(RerankTaskSettings.class));
            assertTrue(((RerankTaskSettings) model.getTaskSettings()).returnDocuments());
        }
    }

    public void testParseRequestConfigEland_PreservesTaskType() {
        var taskType = randomFrom(EnumSet.of(TaskType.RERANK, TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING));
        var modelConfig = mock(TrainedModelConfig.class);
        switch (taskType) {
            case RERANK -> when(modelConfig.getInferenceConfig()).thenReturn(mock(TextSimilarityConfig.class));
            case SPARSE_EMBEDDING -> when(modelConfig.getInferenceConfig()).thenReturn(mock(TextExpansionConfig.class));
            case TEXT_EMBEDDING -> when(modelConfig.getInferenceConfig()).thenReturn(mock(TextEmbeddingConfig.class));
        }

        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetTrainedModelsAction.Response> listener = (ActionListener<GetTrainedModelsAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(new GetTrainedModelsAction.Response(new QueryPage<>(List.of(modelConfig), 1, mock(ParseField.class))));
            return Void.TYPE;
        }).when(client).execute(eq(GetTrainedModelsAction.INSTANCE), any(), any());
        when(client.threadPool()).thenReturn(threadPool);

        var service = createService(client);
        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(
                    ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElasticsearchInternalServiceSettings.NUM_THREADS,
                    4,
                    ElasticsearchInternalServiceSettings.MODEL_ID,
                    "custom-model"
                )
            )
        );

        CustomElandModel expectedModel = getCustomElandModel(taskType);

        PlainActionFuture<Model> listener = new PlainActionFuture<>();
        service.parseRequestConfig(randomInferenceEntityId, taskType, settings, listener);
        var model = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(model, is(expectedModel));
    }

    private CustomElandModel getCustomElandModel(TaskType taskType) {
        CustomElandModel expectedModel = null;
        if (taskType == TaskType.RERANK) {
            expectedModel = new CustomElandRerankModel(
                randomInferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                new CustomElandInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 4, "custom-model", null, null)),
                RerankTaskSettings.DEFAULT_SETTINGS
            );
        } else if (taskType == TaskType.TEXT_EMBEDDING) {
            var serviceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
                1,
                4,
                "custom-model",
                null,
                null,
                null,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            );

            expectedModel = new CustomElandEmbeddingModel(
                randomInferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                serviceSettings,
                null
            );
        } else if (taskType == TaskType.SPARSE_EMBEDDING) {
            expectedModel = new CustomElandModel(
                randomInferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                new CustomElandInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 4, "custom-model", null, null)),
                (ChunkingSettings) null
            );
        }
        return expectedModel;
    }

    public void testBuildInferenceRequest() {
        var id = randomAlphaOfLength(5);
        var inputs = randomList(1, 3, () -> randomAlphaOfLength(4));
        var inputType = InputTypeTests.randomWithoutUnspecified();
        var timeout = randomTimeValue();
        var request = ElasticsearchInternalService.buildInferenceRequest(
            id,
            TextEmbeddingConfigUpdate.EMPTY_INSTANCE,
            inputs,
            inputType,
            timeout
        );

        assertEquals(id, request.getId());
        assertEquals(inputs, request.getTextInput());
        if (inputType == InputType.INTERNAL_INGEST || inputType == InputType.INGEST) {
            assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, request.getPrefixType());
        } else if (inputType == InputType.INTERNAL_SEARCH || inputType == InputType.SEARCH) {
            assertEquals(TrainedModelPrefixStrings.PrefixType.SEARCH, request.getPrefixType());
        } else {
            assertEquals(TrainedModelPrefixStrings.PrefixType.NONE, request.getPrefixType());
        }

        assertEquals(timeout, request.getInferenceTimeout());
        assertEquals(false, request.isChunked());
    }

    @SuppressWarnings("unchecked")
    public void testPutModel() {
        var client = mock(Client.class);
        ArgumentCaptor<PutTrainedModelAction.Request> argument = ArgumentCaptor.forClass(PutTrainedModelAction.Request.class);

        doAnswer(invocation -> {
            var listener = (ActionListener<PutTrainedModelAction.Response>) invocation.getArguments()[2];
            listener.onResponse(new PutTrainedModelAction.Response(mock(TrainedModelConfig.class)));
            return null;
        }).when(client).execute(Mockito.same(PutTrainedModelAction.INSTANCE), argument.capture(), any());

        when(client.threadPool()).thenReturn(threadPool);

        var service = createService(client);

        var model = new MultilingualE5SmallModel(
            "my-e5",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, ".multilingual-e5-small", null),
            null
        );

        service.putModel(model, new ActionListener<>() {
            @Override
            public void onResponse(Boolean success) {
                assertTrue(success);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        var putConfig = argument.getValue().getTrainedModelConfig();
        assertEquals("text_field", putConfig.getInput().getFieldNames().get(0));
    }

    public void testModelVariantDoesNotMatchArchitecturesAndIsNotPlatformAgnostic() {
        {
            assertFalse(
                ElasticsearchInternalService.modelVariantValidForArchitecture(
                    BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC,
                    MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86
                )
            );

            assertTrue(
                ElasticsearchInternalService.modelVariantValidForArchitecture(
                    BaseElasticsearchInternalService.PreferredModelVariant.PLATFORM_AGNOSTIC,
                    MULTILINGUAL_E5_SMALL_MODEL_ID
                )
            );
        }
        {
            assertTrue(
                ElasticsearchInternalService.modelVariantValidForArchitecture(
                    BaseElasticsearchInternalService.PreferredModelVariant.LINUX_X86_OPTIMIZED,
                    MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86
                )
            );
            assertTrue(
                ElasticsearchInternalService.modelVariantValidForArchitecture(
                    BaseElasticsearchInternalService.PreferredModelVariant.LINUX_X86_OPTIMIZED,
                    MULTILINGUAL_E5_SMALL_MODEL_ID
                )
            );
        }
    }

    public void testIsDefaultId() {
        var service = createService(mock(Client.class));
        assertTrue(service.isDefaultId(".elser-2-elasticsearch"));
        assertTrue(service.isDefaultId(".multilingual-e5-small-elasticsearch"));
        assertTrue(service.isDefaultId(".rerank-v1-elasticsearch"));
        assertFalse(service.isDefaultId("foo"));
    }

    public void testGetConfiguration() throws Exception {
        try (var service = createService(mock(Client.class))) {
            String content = XContentHelper.stripWhitespace("""
                {
                       "service": "elasticsearch",
                       "name": "Elasticsearch",
                       "task_types": ["text_embedding", "sparse_embedding", "rerank"],
                       "configurations": {
                           "num_allocations": {
                               "default_value": 1,
                               "description": "The total number of allocations this model is assigned across machine learning nodes.",
                               "label": "Number Allocations",
                               "required": true,
                               "sensitive": false,
                               "updatable": true,
                               "type": "int",
                               "supported_task_types": ["text_embedding", "sparse_embedding", "rerank"]
                           },
                           "num_threads": {
                               "default_value": 2,
                               "description": "Sets the number of threads used by each model allocation during inference.",
                               "label": "Number Threads",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "int",
                               "supported_task_types": ["text_embedding", "sparse_embedding", "rerank"]
                           },
                           "model_id": {
                               "description": "The name of the model to use for the inference task.",
                               "label": "Model ID",
                               "required": true,
                               "sensitive": false,
                               "updatable": false,
                               "type": "str",
                               "supported_task_types": ["text_embedding", "sparse_embedding", "rerank"]
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

    public void testUpdateModelsWithDynamicFields_NoModelsToUpdate() throws Exception {
        ActionListener<List<Model>> resultsListener = ActionListener.<List<Model>>wrap(
            updatedModels -> assertEquals(Collections.emptyList(), updatedModels),
            e -> fail("Unexpected exception: " + e)
        );

        try (var service = createService(mock(Client.class))) {
            service.updateModelsWithDynamicFields(List.of(), resultsListener);
        }
    }

    public void testUpdateModelsWithDynamicFields_InvalidModelProvided() throws IOException {
        ActionListener<List<Model>> resultsListener = ActionListener.wrap(
            updatedModels -> fail("Expected invalid model assertion error to be thrown"),
            e -> fail("Expected invalid model assertion error to be thrown")
        );

        try (var service = createService(mock(Client.class))) {
            assertThrows(
                AssertionError.class,
                () -> { service.updateModelsWithDynamicFields(List.of(mock(Model.class)), resultsListener); }
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testUpdateModelsWithDynamicFields_FailsToRetrieveDeployments() throws IOException {
        var deploymentId = randomAlphaOfLength(10);
        var model = mock(ElasticsearchInternalModel.class);
        when(model.mlNodeDeploymentId()).thenReturn(deploymentId);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        ActionListener<List<Model>> resultsListener = ActionListener.wrap(updatedModels -> {
            assertEquals(updatedModels.size(), 1);
            verify(model).mlNodeDeploymentId();
            verifyNoMoreInteractions(model);
        }, e -> fail("Expected original models to be returned"));

        var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException(randomAlphaOfLength(10)));
            return null;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());

        try (var service = createService(client)) {
            service.updateModelsWithDynamicFields(List.of(model), resultsListener);
        }
    }

    public void testUpdateModelsWithDynamicFields_SingleModelToUpdate() throws IOException {
        var deploymentId = randomAlphaOfLength(10);
        var model = mock(ElasticsearchInternalModel.class);
        when(model.mlNodeDeploymentId()).thenReturn(deploymentId);
        when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        var modelsByDeploymentId = new HashMap<String, List<Model>>();
        modelsByDeploymentId.put(deploymentId, List.of(model));

        testUpdateModelsWithDynamicFields(modelsByDeploymentId);
    }

    public void testUpdateModelsWithDynamicFields_MultipleModelsWithDifferentDeploymentsToUpdate() throws IOException {
        var deploymentId1 = randomAlphaOfLength(10);
        var model1 = mock(ElasticsearchInternalModel.class);
        when(model1.mlNodeDeploymentId()).thenReturn(deploymentId1);
        when(model1.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        var deploymentId2 = randomAlphaOfLength(10);
        var model2 = mock(ElasticsearchInternalModel.class);
        when(model2.mlNodeDeploymentId()).thenReturn(deploymentId2);
        when(model2.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        var modelsByDeploymentId = new HashMap<String, List<Model>>();
        modelsByDeploymentId.put(deploymentId1, List.of(model1));
        modelsByDeploymentId.put(deploymentId2, List.of(model2));

        testUpdateModelsWithDynamicFields(modelsByDeploymentId);
    }

    public void testUpdateModelsWithDynamicFields_MultipleModelsWithSameDeploymentsToUpdate() throws IOException {
        var deploymentId = randomAlphaOfLength(10);
        var model1 = mock(ElasticsearchInternalModel.class);
        when(model1.mlNodeDeploymentId()).thenReturn(deploymentId);
        when(model1.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        var model2 = mock(ElasticsearchInternalModel.class);
        when(model2.mlNodeDeploymentId()).thenReturn(deploymentId);
        when(model2.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);

        var modelsByDeploymentId = new HashMap<String, List<Model>>();
        modelsByDeploymentId.put(deploymentId, List.of(model1, model2));

        testUpdateModelsWithDynamicFields(modelsByDeploymentId);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateModelsWithDynamicFields(Map<String, List<Model>> modelsByDeploymentId) throws IOException {
        var modelsToUpdate = new ArrayList<Model>();
        modelsByDeploymentId.values().forEach(modelsToUpdate::addAll);

        var updatedNumberOfAllocations = new HashMap<String, Integer>();
        modelsByDeploymentId.keySet().forEach(deploymentId -> updatedNumberOfAllocations.put(deploymentId, randomIntBetween(1, 10)));

        ActionListener<List<Model>> resultsListener = ActionListener.wrap(updatedModels -> {
            assertEquals(updatedModels.size(), modelsToUpdate.size());
            modelsByDeploymentId.forEach((deploymentId, models) -> {
                var expectedNumberOfAllocations = updatedNumberOfAllocations.get(deploymentId);
                models.forEach(model -> {
                    verify((ElasticsearchInternalModel) model).updateNumAllocations(expectedNumberOfAllocations);
                    verify((ElasticsearchInternalModel) model).mlNodeDeploymentId();
                    verifyNoMoreInteractions(model);
                });
            });
        }, e -> fail("Unexpected exception: " + e));

        var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocation -> {
            var listener = (ActionListener<GetDeploymentStatsAction.Response>) invocation.getArguments()[2];
            var mockAssignmentStats = new ArrayList<AssignmentStats>();
            modelsByDeploymentId.keySet().forEach(deploymentId -> {
                var mockAssignmentStatsForDeploymentId = mock(AssignmentStats.class);
                when(mockAssignmentStatsForDeploymentId.getDeploymentId()).thenReturn(deploymentId);
                when(mockAssignmentStatsForDeploymentId.getNumberOfAllocations()).thenReturn(updatedNumberOfAllocations.get(deploymentId));
                mockAssignmentStats.add(mockAssignmentStatsForDeploymentId);
            });
            listener.onResponse(
                new GetDeploymentStatsAction.Response(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    mockAssignmentStats,
                    mockAssignmentStats.size()
                )
            );
            return null;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());

        try (var service = createService(client)) {
            service.updateModelsWithDynamicFields(modelsToUpdate, resultsListener);
        }
    }

    public void testUpdateWithoutMlEnabled() throws IOException, InterruptedException {
        var cs = mock(ClusterService.class);
        var cSettings = new ClusterSettings(Settings.EMPTY, Set.of(MachineLearningField.MAX_LAZY_ML_NODES));
        when(cs.getClusterSettings()).thenReturn(cSettings);
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(
            mock(),
            threadPool,
            cs,
            Settings.builder().put("xpack.ml.enabled", false).build()
        );
        try (var service = new ElasticsearchInternalService(context)) {
            var models = List.of(mock(Model.class));
            var latch = new CountDownLatch(1);
            service.updateModelsWithDynamicFields(models, ActionTestUtils.assertNoFailureListener(r -> {
                latch.countDown();
                assertThat(r, Matchers.sameInstance(models));
            }));
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
    }

    public void testUpdateWithMlEnabled() throws IOException, InterruptedException {
        var deploymentId = "deploymentId";
        var model = mock(ElasticsearchInternalModel.class);
        when(model.mlNodeDeploymentId()).thenReturn(deploymentId);

        AssignmentStats stats = mock();
        when(stats.getDeploymentId()).thenReturn(deploymentId);
        when(stats.getNumberOfAllocations()).thenReturn(3);

        var client = mock(Client.class);
        doAnswer(ans -> {
            QueryPage<AssignmentStats> queryPage = new QueryPage<>(List.of(stats), 1, RESULTS_FIELD);

            GetDeploymentStatsAction.Response response = mock();
            when(response.getStats()).thenReturn(queryPage);

            ActionListener<GetDeploymentStatsAction.Response> listener = ans.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(GetDeploymentStatsAction.INSTANCE), any(), any());
        when(client.threadPool()).thenReturn(threadPool);

        var cs = mock(ClusterService.class);
        var cSettings = new ClusterSettings(Settings.EMPTY, Set.of(MachineLearningField.MAX_LAZY_ML_NODES));
        when(cs.getClusterSettings()).thenReturn(cSettings);
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(
            client,
            threadPool,
            cs,
            Settings.builder().put("xpack.ml.enabled", true).build()
        );
        try (var service = new ElasticsearchInternalService(context)) {
            List<Model> models = List.of(model);
            var latch = new CountDownLatch(1);
            service.updateModelsWithDynamicFields(models, ActionTestUtils.assertNoFailureListener(r -> latch.countDown()));
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            verify(model).updateNumAllocations(3);
        }
    }

    public void testStart_OnFailure_WhenTimeoutOccurs() throws IOException {
        var model = new ElserInternalModel(
            "inference_id",
            TaskType.SPARSE_EMBEDDING,
            "elasticsearch",
            new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(1, 1, "id", new AdaptiveAllocationsSettings(false, 0, 0), null)
            ),
            new ElserMlNodeTaskSettings(),
            null
        );

        var client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        doAnswer(invocationOnMock -> {
            ActionListener<GetTrainedModelsAction.Response> listener = invocationOnMock.getArgument(2);
            var builder = GetTrainedModelsAction.Response.builder();
            builder.setModels(List.of(mock(TrainedModelConfig.class)));
            builder.setTotalCount(1);

            listener.onResponse(builder.build());
            return Void.TYPE;
        }).when(client).execute(eq(GetTrainedModelsAction.INSTANCE), any(), any());

        doAnswer(invocationOnMock -> {
            ActionListener<CreateTrainedModelAssignmentAction.Response> listener = invocationOnMock.getArgument(2);
            listener.onFailure(new ElasticsearchStatusException("failed", RestStatus.GATEWAY_TIMEOUT));
            return Void.TYPE;
        }).when(client).execute(eq(StartTrainedModelDeploymentAction.INSTANCE), any(), any());

        try (var service = createService(client)) {
            var actionListener = new PlainActionFuture<Boolean>();
            service.start(model, TimeValue.timeValueSeconds(30), actionListener);
            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> actionListener.actionGet(TimeValue.timeValueSeconds(30))
            );

            assertThat(exception.getMessage(), is("failed"));
        }
    }

    private ElasticsearchInternalService createService(Client client) {
        var cs = mock(ClusterService.class);
        var cSettings = new ClusterSettings(Settings.EMPTY, Set.of(MachineLearningField.MAX_LAZY_ML_NODES));
        when(cs.getClusterSettings()).thenReturn(cSettings);
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client, threadPool, cs, Settings.EMPTY);
        return new ElasticsearchInternalService(context);
    }

    private ElasticsearchInternalService createService(Client client, BaseElasticsearchInternalService.PreferredModelVariant modelVariant) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(
            client,
            threadPool,
            mock(ClusterService.class),
            Settings.EMPTY
        );
        return new ElasticsearchInternalService(context, l -> l.onResponse(modelVariant));
    }
}
