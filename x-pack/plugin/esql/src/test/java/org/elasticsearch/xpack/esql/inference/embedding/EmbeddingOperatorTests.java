/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.esql.inference.AbstractDenseEmbeddingOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;

public class EmbeddingOperatorTests extends AbstractDenseEmbeddingOperatorTestCase {

    @Override
    protected Operator.OperatorFactory createOperatorFactory(InferenceService inferenceService) {
        return new EmbeddingOperator.Factory(
            inferenceService,
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            DataType.TEXT,
            BaseInferenceActionRequest.getDefaultTimeoutForTaskType(TaskType.EMBEDDING)
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("EmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }

    public void testImageEmbeddingOperator() {
        Operator.OperatorFactory factory = new EmbeddingOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            evaluatorFactory(inputChannel),
            DataType.IMAGE,
            BaseInferenceActionRequest.getDefaultTimeoutForTaskType(TaskType.EMBEDDING)
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(imageInput(runner.context().blockFactory(), between(1, 10)));
        runner.run(factory);
    }

    private Page imageInput(org.elasticsearch.compute.data.BlockFactory blockFactory, int size) {
        Block[] blocks = new Block[inputsCount];
        for (int b = 0; b < inputsCount; b++) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(size)) {
                for (int i = 0; i < size; i++) {
                    builder.appendBytesRef(new BytesRef("data:image/jpeg;base64,VGhpcyBpcyBhbiBpbWFnZQ=="));
                }
                blocks[b] = builder.build();
            }
        }
        return new Page(blocks);
    }
}
