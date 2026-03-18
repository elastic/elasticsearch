/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
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
            DataFormat.TEXT,
            InferenceAction.Request.DEFAULT_TIMEOUT
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
            DataFormat.BASE64,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));
        runner.run(factory);
    }
}
