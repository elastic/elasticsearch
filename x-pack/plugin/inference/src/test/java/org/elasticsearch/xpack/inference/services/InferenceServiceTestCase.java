/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerService;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.junit.Assume;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.InferenceString.fromStringList;
import static org.elasticsearch.inference.InferenceStringTests.createRandomUsingDataTypes;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class InferenceServiceTestCase extends ESTestCase {

    public abstract InferenceService createInferenceService();

    public void testRerankersImplementRerankInterface() throws IOException {
        try (InferenceService inferenceService = createInferenceService()) {
            boolean implementsReranking = inferenceService instanceof RerankingInferenceService;
            boolean hasRerankTaskType = supportsRerank(inferenceService);
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
    private static boolean supportsRerank(InferenceService inferenceService) {
        if (inferenceService instanceof ElasticInferenceService) {
            return ElasticInferenceService.IMPLEMENTED_TASK_TYPES.contains(TaskType.RERANK);
        } else {
            return inferenceService.supportedTaskTypes().contains(TaskType.RERANK);
        }
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
