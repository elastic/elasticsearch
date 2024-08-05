/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;
import org.elasticsearch.xpack.core.utils.FloatConversionUtils;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchInternalServiceTests extends ESTestCase {

    TaskType taskType = TaskType.TEXT_EMBEDDING;
    String randomInferenceEntityId = randomAlphaOfLength(10);

    private static ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testParseRequestConfig() {

        // Null model variant
        {
            var service = createService(mock(Client.class));
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(
                    Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(IllegalArgumentException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
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
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID,
                null
            );

            service.parseRequestConfig(
                randomInferenceEntityId,
                taskType,
                settings,
                Set.of(),
                getModelVerificationActionListener(e5ServiceSettings)
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

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
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
                        ElasticsearchInternalServiceSettings.MODEL_ID,
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID, // we can't directly test the eland case until we mock
                                                                                     // the threadpool within the client
                        "not_a_valid_service_setting",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Extra service settings
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
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID, // we can't directly test the eland case until we mock
                                                                                     // the threadpool within the client
                        "extra_setting_that_should_not_be_here",
                        randomAlphaOfLength(10)
                    )
                )
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }

        // Extra settings
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
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID // we can't directly test the eland case until we mock
                        // the threadpool within the client
                    )
                )
            );
            settings.put("extra_setting_that_should_not_be_here", randomAlphaOfLength(10));

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(
                model -> fail("Model parsing should have failed"),
                e -> assertThat(e, instanceOf(ElasticsearchStatusException.class))
            );

            service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), modelListener);
        }
    }

    @SuppressWarnings("unchecked")
    public void testParseRequestConfig_Rerank() {
        // with task settings
        {
            var client = mock(Client.class);
            doAnswer(invocation -> {
                var listener = (ActionListener<GetTrainedModelsAction.Response>) invocation.getArguments()[2];
                listener.onResponse(
                    new GetTrainedModelsAction.Response(new QueryPage<>(List.of(mock(TrainedModelConfig.class)), 1, mock(ParseField.class)))
                );
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
            settings.put(
                ModelConfigurations.TASK_SETTINGS,
                new HashMap<>(Map.of(CustomElandRerankTaskSettings.RETURN_DOCUMENTS, returnDocs))
            );

            ActionListener<Model> modelListener = ActionListener.<Model>wrap(model -> {
                assertThat(model, instanceOf(CustomElandRerankModel.class));
                assertThat(model.getTaskSettings(), instanceOf(CustomElandRerankTaskSettings.class));
                assertThat(model.getServiceSettings(), instanceOf(CustomElandInternalServiceSettings.class));
                assertEquals(returnDocs, ((CustomElandRerankTaskSettings) model.getTaskSettings()).returnDocuments());
            }, e -> { fail("Model parsing failed " + e.getMessage()); });

            service.parseRequestConfig(randomInferenceEntityId, TaskType.RERANK, settings, Set.of(), modelListener);
        }
    }

    @SuppressWarnings("unchecked")
    public void testParseRequestConfig_Rerank_DefaultTaskSettings() {
        // with task settings
        {
            var client = mock(Client.class);
            doAnswer(invocation -> {
                var listener = (ActionListener<GetTrainedModelsAction.Response>) invocation.getArguments()[2];
                listener.onResponse(
                    new GetTrainedModelsAction.Response(new QueryPage<>(List.of(mock(TrainedModelConfig.class)), 1, mock(ParseField.class)))
                );
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
                assertThat(model.getTaskSettings(), instanceOf(CustomElandRerankTaskSettings.class));
                assertThat(model.getServiceSettings(), instanceOf(CustomElandInternalServiceSettings.class));
                assertEquals(Boolean.TRUE, ((CustomElandRerankTaskSettings) model.getTaskSettings()).returnDocuments());
            }, e -> { fail("Model parsing failed " + e.getMessage()); });

            service.parseRequestConfig(randomInferenceEntityId, TaskType.RERANK, settings, Set.of(), modelListener);
        }
    }

    private ActionListener<Model> getModelVerificationActionListener(MultilingualE5SmallInternalServiceSettings e5ServiceSettings) {
        return ActionListener.<Model>wrap(model -> {
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, ElasticsearchInternalService.NAME, e5ServiceSettings),
                model
            );
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

            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));

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
                taskType,
                settings
            );
            var elandServiceSettings = new CustomElandInternalTextEmbeddingServiceSettings(1, 4, "invalid", null);
            assertEquals(
                new CustomElandEmbeddingModel(randomInferenceEntityId, taskType, ElasticsearchInternalService.NAME, elandServiceSettings),
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
                        ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID,
                        ServiceFields.DIMENSIONS,
                        1
                    )
                )
            );

            var e5ServiceSettings = new MultilingualE5SmallInternalServiceSettings(
                1,
                4,
                ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID,
                null
            );

            MultilingualE5SmallModel parsedModel = (MultilingualE5SmallModel) service.parsePersistedConfig(
                randomInferenceEntityId,
                taskType,
                settings
            );
            assertEquals(
                new MultilingualE5SmallModel(randomInferenceEntityId, taskType, ElasticsearchInternalService.NAME, e5ServiceSettings),
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
            expectThrows(IllegalArgumentException.class, () -> service.parsePersistedConfig(randomInferenceEntityId, taskType, settings));
        }
    }

    @SuppressWarnings("unchecked")
    public void testChunkInfer() {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(MlChunkedTextEmbeddingFloatResultsTests.createRandomResults());
        mlTrainedModelResults.add(MlChunkedTextEmbeddingFloatResultsTests.createRandomResults());
        mlTrainedModelResults.add(new ErrorInferenceResults(new RuntimeException("boom")));
        var response = new InferModelAction.Response(mlTrainedModelResults, "foo", true);

        ThreadPool threadpool = new TestThreadPool("test");
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadpool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferModelAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(InferModelAction.Request.class), any(ActionListener.class));

        var model = new MultilingualE5SmallModel(
            "foo",
            TaskType.TEXT_EMBEDDING,
            "e5",
            new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null)
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInferenceServiceResults>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(3));
            assertThat(chunkedResponse.get(0), instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
            var result1 = (InferenceChunkedTextEmbeddingFloatResults) chunkedResponse.get(0);
            assertEquals(
                ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(0)).getChunks().size(),
                result1.getChunks().size()
            );
            assertEquals(
                ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(0)).getChunks().get(0).matchedText(),
                result1.getChunks().get(0).matchedText()
            );
            assertArrayEquals(
                (FloatConversionUtils.floatArrayOf(
                    ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(0)).getChunks().get(0).embedding()
                )),
                result1.getChunks().get(0).embedding(),
                0.0001f
            );
            assertThat(chunkedResponse.get(1), instanceOf(InferenceChunkedTextEmbeddingFloatResults.class));
            var result2 = (InferenceChunkedTextEmbeddingFloatResults) chunkedResponse.get(1);
            // assertEquals(((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(1)).getChunks(), result2.getChunks());

            assertEquals(
                ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(1)).getChunks().size(),
                result2.getChunks().size()
            );
            assertEquals(
                ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(1)).getChunks().get(0).matchedText(),
                result2.getChunks().get(0).matchedText()
            );
            assertArrayEquals(
                (FloatConversionUtils.floatArrayOf(
                    ((MlChunkedTextEmbeddingFloatResults) mlTrainedModelResults.get(1)).getChunks().get(0).embedding()
                )),
                result2.getChunks().get(0).embedding(),
                0.0001f
            );

            var result3 = (ErrorChunkedInferenceResults) chunkedResponse.get(2);
            assertThat(result3.getException(), instanceOf(RuntimeException.class));
            assertThat(result3.getException().getMessage(), containsString("boom"));
            gotResults.set(true);
        }, ESTestCase::fail);

        service.chunkedInfer(
            model,
            null,
            List.of("foo", "bar"),
            Map.of(),
            InputType.SEARCH,
            new ChunkingOptions(null, null),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            ActionListener.runAfter(resultsListener, () -> terminate(threadpool))
        );

        if (gotResults.get() == false) {
            terminate(threadpool);
        }
        assertTrue("Listener not called", gotResults.get());
    }

    @SuppressWarnings("unchecked")
    public void testChunkInferSetsTokenization() {
        var expectedSpan = new AtomicInteger();
        var expectedWindowSize = new AtomicReference<Integer>();

        Client client = mock(Client.class);
        ThreadPool threadpool = new TestThreadPool("test");
        try {
            when(client.threadPool()).thenReturn(threadpool);
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
                new MultilingualE5SmallInternalServiceSettings(1, 1, "cross-platform", null)
            );
            var service = createService(client);

            expectedSpan.set(-1);
            expectedWindowSize.set(null);
            service.chunkedInfer(
                model,
                List.of("foo", "bar"),
                Map.of(),
                InputType.SEARCH,
                null,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                ActionListener.wrap(r -> fail("unexpected result"), e -> fail(e.getMessage()))
            );

            expectedSpan.set(-1);
            expectedWindowSize.set(256);
            service.chunkedInfer(
                model,
                List.of("foo", "bar"),
                Map.of(),
                InputType.SEARCH,
                new ChunkingOptions(256, null),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                ActionListener.wrap(r -> fail("unexpected result"), e -> fail(e.getMessage()))
            );
        } finally {
            terminate(threadpool);
        }
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
            settings.put(
                ModelConfigurations.TASK_SETTINGS,
                new HashMap<>(Map.of(CustomElandRerankTaskSettings.RETURN_DOCUMENTS, returnDocs))
            );

            var model = service.parsePersistedConfig(randomInferenceEntityId, TaskType.RERANK, settings);
            assertThat(model.getTaskSettings(), instanceOf(CustomElandRerankTaskSettings.class));
            assertEquals(returnDocs, ((CustomElandRerankTaskSettings) model.getTaskSettings()).returnDocuments());
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
            assertThat(model.getTaskSettings(), instanceOf(CustomElandRerankTaskSettings.class));
            assertTrue(((CustomElandRerankTaskSettings) model.getTaskSettings()).returnDocuments());
        }
    }

    public void testParseRequestConfigEland_PreservesTaskType() {
        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetTrainedModelsAction.Response> listener = (ActionListener<GetTrainedModelsAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(
                new GetTrainedModelsAction.Response(new QueryPage<>(List.of(mock(TrainedModelConfig.class)), 1, mock(ParseField.class)))
            );
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

        var taskType = randomFrom(EnumSet.of(TaskType.RERANK, TaskType.TEXT_EMBEDDING));
        CustomElandModel expectedModel = getCustomElandModel(taskType);

        PlainActionFuture<Model> listener = new PlainActionFuture<>();
        service.parseRequestConfig(randomInferenceEntityId, taskType, settings, Set.of(), listener);
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
                new CustomElandInternalServiceSettings(1, 4, "custom-model", null),
                CustomElandRerankTaskSettings.DEFAULT_SETTINGS
            );
        } else if (taskType == TaskType.TEXT_EMBEDDING) {
            var serviceSettings = new CustomElandInternalTextEmbeddingServiceSettings(1, 4, "custom-model", null);

            expectedModel = new CustomElandEmbeddingModel(
                randomInferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                serviceSettings
            );
        }
        return expectedModel;
    }

    public void testBuildInferenceRequest() {
        var id = randomAlphaOfLength(5);
        var inputs = randomList(1, 3, () -> randomAlphaOfLength(4));
        var inputType = randomFrom(InputType.SEARCH, InputType.INGEST);
        var timeout = randomTimeValue();
        var chunk = randomBoolean();
        var request = ElasticsearchInternalService.buildInferenceRequest(
            id,
            TextEmbeddingConfigUpdate.EMPTY_INSTANCE,
            inputs,
            inputType,
            timeout,
            chunk
        );

        assertEquals(id, request.getId());
        assertEquals(inputs, request.getTextInput());
        assertEquals(
            inputType == InputType.INGEST ? TrainedModelPrefixStrings.PrefixType.INGEST : TrainedModelPrefixStrings.PrefixType.SEARCH,
            request.getPrefixType()
        );
        assertEquals(timeout, request.getInferenceTimeout());
        assertEquals(chunk, request.isChunked());
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
            new MultilingualE5SmallInternalServiceSettings(1, 1, ".multilingual-e5-small", null)
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

    public void testParseRequestConfigEland_SetsDimensionsToOne() {
        var client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(
                new InferModelAction.Response(List.of(new MlTextEmbeddingResults("field", new double[] { 0.1 }, false)), "id", true)
            );

            var request = (InferModelAction.Request) invocationOnMock.getArguments()[1];
            assertThat(request.getId(), is("custom-model"));
            return Void.TYPE;
        }).when(client).execute(eq(InferModelAction.INSTANCE), any(), any());
        when(client.threadPool()).thenReturn(threadPool);

        var service = createService(client);

        var serviceSettings = new CustomElandInternalTextEmbeddingServiceSettings(
            1,
            4,
            "custom-model",
            null,
            1,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        var taskType = TaskType.TEXT_EMBEDDING;
        var expectedModel = new CustomElandEmbeddingModel(
            randomInferenceEntityId,
            taskType,
            ElasticsearchInternalService.NAME,
            serviceSettings
        );

        PlainActionFuture<Model> listener = new PlainActionFuture<>();
        service.checkModelConfig(
            new CustomElandEmbeddingModel(
                randomInferenceEntityId,
                taskType,
                ElasticsearchInternalService.NAME,
                new CustomElandInternalTextEmbeddingServiceSettings(
                    1,
                    4,
                    "custom-model",
                    null,
                    null,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            ),
            listener
        );
        var model = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(model, is(expectedModel));
    }

    private ElasticsearchInternalService createService(Client client) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client);
        return new ElasticsearchInternalService(context);
    }

    public static Model randomModelConfig(String inferenceEntityId) {
        List<String> givenList = Arrays.asList("MultilingualE5SmallModel");
        Random rand = org.elasticsearch.common.Randomness.get();
        String model = givenList.get(rand.nextInt(givenList.size()));

        return switch (model) {
            case "MultilingualE5SmallModel" -> new MultilingualE5SmallModel(
                inferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                ElasticsearchInternalService.NAME,
                MultilingualE5SmallInternalServiceSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("model " + model + " is not supported for testing");
        };
    }

}
