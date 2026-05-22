/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerService;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceString.fromStringList;
import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is for tests that apply to all implementations of {@link InferenceService} but which are not parameterized.
 */
public abstract class InferenceServiceTestCase extends ESTestCase {
    public final MockWebServer webServer = new MockWebServer();
    public ThreadPool threadPool;
    public HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        doInit();
    }

    /**
     * This method should be overridden by implementations that need to do non-standard set-up
     */
    public void doInit() throws IOException {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        doShutdown();
    }

    /**
     * This method should be overridden by implementations that need to do non-standard shutdown
     */
    public void doShutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public abstract InferenceService createInferenceService();

    public void testInfer_ThrowsErrorWhenModelIsNotValid() throws IOException {
        try (var service = createInferenceService()) {
            var listener = new PlainActionFuture<InferenceServiceResults>();

            service.infer(
                getInvalidModel("id", "service"),
                null,
                null,
                null,
                List.of(""),
                false,
                new HashMap<>(),
                InputType.UNSPECIFIED,
                null,
                listener
            );

            var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                exception.getMessage(),
                is("The internal model was invalid, please delete the service [service] with id [id] and add it again.")
            );
        }
    }

    // reranker tests
    public void testRerankersImplementRerankInterface() throws IOException {
        try (InferenceService inferenceService = createInferenceService()) {
            boolean implementsReranking = inferenceService instanceof RerankingInferenceService;
            boolean hasRerankTaskType = supportsTaskType(inferenceService, TaskType.RERANK);
            if (implementsReranking != hasRerankTaskType) {
                fail(
                    "Reranking inference services should implement RerankingInferenceService and support the RERANK task type. "
                        + "Service ["
                        + inferenceService.name()
                        + "] supports task type: ["
                        + hasRerankTaskType
                        + "] and implements"
                        + " RerankingInferenceService: ["
                        + implementsReranking
                        + "]"
                );
            }
        }
    }

    // This method is necessary because ElasticInferenceService.supportedTaskTypes() throws
    private static boolean supportsTaskType(InferenceService inferenceService, TaskType taskType) {
        EnumSet<TaskType> supportedTaskTypes;
        if (inferenceService instanceof ElasticInferenceService) {
            supportedTaskTypes = ElasticInferenceService.IMPLEMENTED_TASK_TYPES;
        } else {
            supportedTaskTypes = inferenceService.supportedTaskTypes();
        }
        return supportedTaskTypes.contains(taskType);
    }

    public void testRerankersHaveWindowSize() throws IOException {
        try (InferenceService inferenceService = createInferenceService()) {
            if (inferenceService instanceof RerankingInferenceService rerankingInferenceService) {
                assertRerankerWindowSize(rerankingInferenceService);
            }
        }
    }

    protected void assertRerankerWindowSize(RerankingInferenceService rerankingInferenceService) {
        fail("Reranking services should override this test method to verify window size");
    }

    // dense embedding tests
    public void testUpdateModelWithEmbeddingDetails_InvalidModelProvided_Throws() throws IOException {
        try (InferenceService inferenceService = createInferenceService()) {
            boolean supportsEmbeddingTaskType = supportsTaskType(inferenceService, TaskType.TEXT_EMBEDDING)
                || supportsTaskType(inferenceService, TaskType.EMBEDDING);
            Assume.assumeTrue(supportsEmbeddingTaskType);

            var exception = expectThrows(
                ElasticsearchStatusException.class,
                () -> inferenceService.updateModelWithEmbeddingDetails(getInvalidModel("id", "service"), randomNonNegativeInt())
            );

            assertThat(exception.getMessage(), containsString("Can't update embedding details for model"));
        }
    }

    public void testUpdateModelWithEmbeddingDetails_NullSimilarityInOriginalModel_UsesDefaultSimilarity() throws IOException {
        testUpdateModelWithEmbeddingDetails(null, getDefaultSimilarity());
    }

    public void testUpdateModelWithEmbeddingDetails_NonNullSimilarityInOriginalModel_KeepsSimilarity() throws IOException {
        testUpdateModelWithEmbeddingDetails(SimilarityMeasure.COSINE, SimilarityMeasure.COSINE);
    }

    private void testUpdateModelWithEmbeddingDetails(SimilarityMeasure similarityInModel, SimilarityMeasure expectedSimilarity)
        throws IOException {
        try (var inferenceService = createInferenceService()) {
            boolean supportsEmbeddingTaskType = supportsTaskType(inferenceService, TaskType.TEXT_EMBEDDING)
                || supportsTaskType(inferenceService, TaskType.EMBEDDING);
            Assume.assumeTrue(supportsEmbeddingTaskType);

            var embeddingSize = randomNonNegativeInt();
            var model = createEmbeddingModel(similarityInModel);

            Model updatedModel = inferenceService.updateModelWithEmbeddingDetails(model, embeddingSize);

            assertEquals(expectedSimilarity, updatedModel.getServiceSettings().similarity());
            assertEquals(embeddingSize, updatedModel.getServiceSettings().dimensions().intValue());
        }
    }

    /**
     * This should be overridden by services that support {@link TaskType#TEXT_EMBEDDING} or {@link TaskType#EMBEDDING} to return a dense
     * embedding model with the provided {@link SimilarityMeasure}
     * @param similarity the {@link SimilarityMeasure} to use in the returned model
     * @return a dense embedding model
     */
    public Model createEmbeddingModel(@Nullable SimilarityMeasure similarity) {
        fail("Services that support dense embedding should override this method to return a dense embedding model");
        return null;
    }

    /**
     * This should be overridden by services that do not use {@link SimilarityMeasure#DOT_PRODUCT} as their default similarity
     * @return the default {@link SimilarityMeasure} for the service
     */
    public SimilarityMeasure getDefaultSimilarity() {
        return SimilarityMeasure.DOT_PRODUCT;
    }

    // streaming tests
    /**
     * This should be overridden to return a set of the {@link TaskType} that support streaming for the service if they are different from
     * {@link TaskType#COMPLETION} and {@link TaskType#CHAT_COMPLETION}, which is the most common case.
     * @return a set of {@link TaskType}
     */
    public EnumSet<TaskType> expectedStreamingTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    public void testSupportedStreamingTasks() throws Exception {
        try (var service = createInferenceService()) {
            assertThat(service.supportedStreamingTasks(), is(expectedStreamingTasks()));
            assertFalse(service.canStream(TaskType.ANY));
        }
    }

    public void testChunkedInferDoesNotSupportMultipleItemsPerContent() throws IOException {
        try (var inferenceService = createInferenceService()) {
            var multipleItemsInput = new InferenceStringGroup(fromStringList(List.of("input1", "input2")));
            var exceptionMessage = Strings.format(
                "Field [content] must contain a single item for [%s] service. "
                    + "[content] object with multiple items found at $.input.content[0]",
                inferenceService.name()
            );
            testChunkedInferFailure(inferenceService, multipleItemsInput, exceptionMessage);
        }
    }

    public void testChunkedInferDoesNotSupportNonTextInputs() throws IOException {
        try (var inferenceService = createInferenceService()) {
            Assume.assumeFalse(inferenceService.supportsNonTextEmbeddingContent());

            var nonTextInput = new InferenceStringGroup(createRandomUsingDataTypes(EnumSet.complementOf(EnumSet.of(DataType.TEXT))));
            var exceptionMessage = Strings.format(
                "The %s service does not support embedding with non-text inputs",
                inferenceService.name()
            );
            testChunkedInferFailure(inferenceService, nonTextInput, exceptionMessage);
        }
    }

    private static void testChunkedInferFailure(
        InferenceService inferenceService,
        InferenceStringGroup inferenceStrings,
        String exceptionMessage
    ) {
        Assume.assumeTrue(inferenceService.supportsChunkedInfer());

        var inputs = List.of(new ChunkInferenceInput(inferenceStrings, null));
        var listener = new TestPlainActionFuture<List<ChunkedInference>>();

        Model model;
        // SageMakerService and ElasticsearchInternalService check the type of the model in chunkedInfer(), so ensure we're using the
        // appropriate type
        if (inferenceService instanceof SageMakerService) {
            model = mock(SageMakerModel.class);
        } else if (inferenceService instanceof ElasticsearchInternalService) {
            model = mock(ElasticInferenceServiceModel.class);
            when(model.getTaskType()).thenReturn(TaskType.TEXT_EMBEDDING);
        } else {
            model = mock();
        }

        inferenceService.chunkedInfer(model, null, inputs, Map.of(), InputType.INTERNAL_INGEST, ESTestCase.TEST_REQUEST_TIMEOUT, listener);
        var exception = expectThrows(ElasticsearchStatusException.class, listener::actionGet);

        assertThat(exception.status(), is(BAD_REQUEST));
        assertThat(exception.getMessage(), is(exceptionMessage));
    }
}
