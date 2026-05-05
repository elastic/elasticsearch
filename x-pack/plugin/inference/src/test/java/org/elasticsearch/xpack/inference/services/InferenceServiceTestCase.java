/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.junit.Assume;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.elasticsearch.xpack.inference.Utils.getInvalidModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * This class is for tests that apply to all implementations of {@link InferenceService} but which are not parameterized.
 */
public abstract class InferenceServiceTestCase extends ESTestCase {

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
}
