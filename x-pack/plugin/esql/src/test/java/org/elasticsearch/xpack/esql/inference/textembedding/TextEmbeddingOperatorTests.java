/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.inference.AbstractDenseEmbeddingOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;

public class TextEmbeddingOperatorTests extends AbstractDenseEmbeddingOperatorTestCase {

    @Override
    protected Operator.OperatorFactory createOperatorFactory(InferenceService inferenceService) {
        return new TextEmbeddingOperator.Factory(inferenceService, SIMPLE_INFERENCE_ID, evaluatorFactory(inputChannel), null);
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("TextEmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }
}
