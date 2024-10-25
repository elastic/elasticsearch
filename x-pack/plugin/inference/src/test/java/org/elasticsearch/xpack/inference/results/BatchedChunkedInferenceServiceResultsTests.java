/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.BatchedChunkedInferenceServiceResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchedChunkedInferenceServiceResultsTests extends AbstractWireSerializingTestCase<BatchedChunkedInferenceServiceResults> {
    @Override
    protected Writeable.Reader<BatchedChunkedInferenceServiceResults> instanceReader() {
        return BatchedChunkedInferenceServiceResults::new;
    }

    @Override
    protected BatchedChunkedInferenceServiceResults createTestInstance() {
        var taskType = randomFrom(List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING));
        return new BatchedChunkedInferenceServiceResults(taskType, createRandomChunkedInferenceServiceResults(taskType));
    }

    private List<ChunkedInferenceServiceResults> createRandomChunkedInferenceServiceResults(TaskType taskType) {
        var results = new ArrayList<ChunkedInferenceServiceResults>();
        var resultsCount = randomIntBetween(1, 5);

        if (TaskType.TEXT_EMBEDDING.equals(taskType)) {
            for (int i = 0; i < resultsCount; i++) {
                results.add(InferenceChunkedTextEmbeddingByteResultsTests.createRandomResults());
            }
        } else {
            for (int i = 0; i < resultsCount; i++) {
                results.add(InferenceChunkedSparseEmbeddingResultsTests.createRandomResults());
            }
        }

        return results;
    }

    @Override
    protected BatchedChunkedInferenceServiceResults mutateInstance(BatchedChunkedInferenceServiceResults instance) throws IOException {
        var taskType = randomFrom(List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING));
        var results = randomValueOtherThan(
            instance.getChunkedInferenceServiceResults(),
            () -> createRandomChunkedInferenceServiceResults(taskType)
        );

        return new BatchedChunkedInferenceServiceResults(taskType, results);
    }
}
