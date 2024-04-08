/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResultsTests;
import org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElserInternalServiceTests extends ESTestCase {

    public static Model randomModelConfig(String inferenceEntityId, TaskType taskType) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new ElserInternalModel(
                inferenceEntityId,
                taskType,
                ElserInternalService.NAME,
                ElserInternalServiceSettingsTests.createRandom(),
                ElserMlNodeTaskSettingsTests.createRandom()
            );
            default -> throw new IllegalArgumentException("task type " + taskType + " is not supported");
        };
    }

    public void testParseConfigStrict() {
        var service = createService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(
                    ElserInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElserInternalServiceSettings.NUM_THREADS,
                    4,
                    "model_id",
                    ".elser_model_1"
                )
            )
        );
        settings.put(ModelConfigurations.TASK_SETTINGS, Map.of());

        var expectedModel = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElserInternalService.NAME,
            new ElserInternalServiceSettings(1, 4, ".elser_model_1"),
            ElserMlNodeTaskSettings.DEFAULT
        );

        var modelVerificationListener = getModelVerificationListener(expectedModel);

        service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelVerificationListener);

    }

    public void testParseConfigLooseWithOldModelId() {
        var service = createService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(
                Map.of(
                    ElserInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElserInternalServiceSettings.NUM_THREADS,
                    4,
                    "model_version",
                    ".elser_model_1"
                )
            )
        );
        settings.put(ModelConfigurations.TASK_SETTINGS, Map.of());

        var expectedModel = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElserInternalService.NAME,
            new ElserInternalServiceSettings(1, 4, ".elser_model_1"),
            ElserMlNodeTaskSettings.DEFAULT
        );

        var realModel = service.parsePersistedConfig("foo", TaskType.SPARSE_EMBEDDING, settings);

        assertEquals(expectedModel, realModel);

    }

    private static ActionListener<Model> getModelVerificationListener(ElserInternalModel expectedModel) {
        return ActionListener.<Model>wrap(
            (model) -> { assertEquals(expectedModel, model); },
            (e) -> fail("Model verification should not fail " + e.getMessage())
        );
    }

    public void testParseConfigStrictWithNoTaskSettings() {
        var service = createService(mock(Client.class));

        var settings = new HashMap<String, Object>();
        settings.put(
            ModelConfigurations.SERVICE_SETTINGS,
            new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 1, ElserInternalServiceSettings.NUM_THREADS, 4))
        );

        var expectedModel = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            ElserInternalService.NAME,
            new ElserInternalServiceSettings(1, 4, ElserInternalService.ELSER_V2_MODEL),
            ElserMlNodeTaskSettings.DEFAULT
        );

        var modelVerificationListener = getModelVerificationListener(expectedModel);

        service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelVerificationListener);

    }

    public void testParseConfigStrictWithUnknownSettings() {

        var service = createService(mock(Client.class));

        for (boolean throwOnUnknown : new boolean[] { true, false }) {
            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserInternalServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserInternalServiceSettings.NUM_THREADS,
                            4,
                            ElserInternalServiceSettings.MODEL_ID,
                            ".elser_model_2"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of());
                settings.put("foo", "bar");

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });

                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {

                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserInternalServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserInternalServiceSettings.NUM_THREADS,
                            4,
                            ElserInternalServiceSettings.MODEL_ID,
                            ".elser_model_2"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of("foo", "bar"));

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });
                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {
                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }

            {
                var settings = new HashMap<String, Object>();
                settings.put(
                    ModelConfigurations.SERVICE_SETTINGS,
                    new HashMap<>(
                        Map.of(
                            ElserInternalServiceSettings.NUM_ALLOCATIONS,
                            1,
                            ElserInternalServiceSettings.NUM_THREADS,
                            4,
                            ElserInternalServiceSettings.MODEL_ID,
                            ".elser_model_2",
                            "foo",
                            "bar"
                        )
                    )
                );
                settings.put(ModelConfigurations.TASK_SETTINGS, Map.of("foo", "bar"));

                ActionListener<Model> errorVerificationListener = ActionListener.wrap((model) -> {
                    if (throwOnUnknown) {
                        fail("Model verification should fail when throwOnUnknown is true");
                    }
                }, (e) -> {
                    if (throwOnUnknown) {
                        assertThat(
                            e.getMessage(),
                            containsString("Model configuration contains settings [{foo=bar}] unknown to the [elser] service")
                        );
                    } else {
                        fail("Model verification should not fail when throwOnUnknown is false");
                    }
                });
                if (throwOnUnknown == false) {
                    var parsed = service.parsePersistedConfigWithSecrets(
                        "foo",
                        TaskType.SPARSE_EMBEDDING,
                        settings,
                        Collections.emptyMap()
                    );
                } else {
                    service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), errorVerificationListener);
                }
            }
        }
    }

    public void testParseRequestConfig_DefaultModel() {
        var service = createService(mock(Client.class));
        {
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 1, ElserInternalServiceSettings.NUM_THREADS, 4))
            );

            ActionListener<Model> modelActionListener = ActionListener.<Model>wrap((model) -> {
                assertEquals(".elser_model_2", ((ElserInternalModel) model).getServiceSettings().getModelId());
            }, (e) -> { fail("Model verification should not fail"); });

            service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of(), modelActionListener);
        }
        {
            var settings = new HashMap<String, Object>();
            settings.put(
                ModelConfigurations.SERVICE_SETTINGS,
                new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 1, ElserInternalServiceSettings.NUM_THREADS, 4))
            );

            ActionListener<Model> modelActionListener = ActionListener.<Model>wrap((model) -> {
                assertEquals(".elser_model_2_linux-x86_64", ((ElserInternalModel) model).getServiceSettings().getModelId());
            }, (e) -> { fail("Model verification should not fail"); });

            service.parseRequestConfig("foo", TaskType.SPARSE_EMBEDDING, settings, Set.of("linux-x86_64"), modelActionListener);
        }
    }

    @SuppressWarnings("unchecked")
    public void testChunkInfer() {
        var mlTrainedModelResults = new ArrayList<InferenceResults>();
        mlTrainedModelResults.add(ChunkedTextExpansionResultsTests.createRandomResults());
        mlTrainedModelResults.add(ChunkedTextExpansionResultsTests.createRandomResults());
        mlTrainedModelResults.add(new ErrorInferenceResults(new RuntimeException("boom")));
        var response = new InferTrainedModelDeploymentAction.Response(mlTrainedModelResults);

        ThreadPool threadpool = new TestThreadPool("test");
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadpool);
        doAnswer(invocationOnMock -> {
            var listener = (ActionListener<InferTrainedModelDeploymentAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        }).when(client)
            .execute(
                same(InferTrainedModelDeploymentAction.INSTANCE),
                any(InferTrainedModelDeploymentAction.Request.class),
                any(ActionListener.class)
            );

        var model = new ElserInternalModel(
            "foo",
            TaskType.SPARSE_EMBEDDING,
            "elser",
            new ElserInternalServiceSettings(1, 1, "elser"),
            new ElserMlNodeTaskSettings()
        );
        var service = createService(client);

        var gotResults = new AtomicBoolean();
        var resultsListener = ActionListener.<List<ChunkedInferenceServiceResults>>wrap(chunkedResponse -> {
            assertThat(chunkedResponse, hasSize(3));
            assertThat(chunkedResponse.get(0), instanceOf(ChunkedSparseEmbeddingResults.class));
            var result1 = (ChunkedSparseEmbeddingResults) chunkedResponse.get(0);
            assertEquals(
                ((org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults) mlTrainedModelResults.get(0)).getChunks(),
                result1.getChunkedResults()
            );
            assertThat(chunkedResponse.get(1), instanceOf(ChunkedSparseEmbeddingResults.class));
            var result2 = (ChunkedSparseEmbeddingResults) chunkedResponse.get(1);
            assertEquals(
                ((org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults) mlTrainedModelResults.get(1)).getChunks(),
                result2.getChunkedResults()
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

        ThreadPool threadpool = new TestThreadPool("test");
        Client client = mock(Client.class);
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

            var model = new ElserInternalModel(
                "foo",
                TaskType.SPARSE_EMBEDDING,
                "elser",
                new ElserInternalServiceSettings(1, 1, "elser"),
                new ElserMlNodeTaskSettings()
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
                ActionListener.wrap(r -> fail("unexpected result"), e -> fail(e.getMessage()))
            );
        } finally {
            terminate(threadpool);
        }
    }

    private ElserInternalService createService(Client client) {
        var context = new InferenceServiceExtension.InferenceServiceFactoryContext(client);
        return new ElserInternalService(context);
    }
}
