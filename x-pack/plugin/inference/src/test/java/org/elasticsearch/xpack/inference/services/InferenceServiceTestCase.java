/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.io.IOException;

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
}
