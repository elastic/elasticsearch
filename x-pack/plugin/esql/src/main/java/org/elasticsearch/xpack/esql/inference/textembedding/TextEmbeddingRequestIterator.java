/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.AbstractEmbeddingRequestIterator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.List;

/**
 * Embedding request iterator for plain (untyped) text inputs.
 * <p>
 * Produces {@link InferenceAction.Request} items
 * </p>
 */
class TextEmbeddingRequestIterator extends AbstractEmbeddingRequestIterator {

    TextEmbeddingRequestIterator(String inferenceId, BytesRefBlock textBlock) {
        super(inferenceId, TaskType.TEXT_EMBEDDING, textBlock);
    }

    @Override
    protected BulkInferenceRequestItem buildRequestItem(String text, PositionValueCountsBuilder pvcs) {
        if (text == null) {
            return new BulkInferenceRequestItem(null, pvcs);
        }
        return new BulkInferenceRequestItem(InferenceAction.Request.builder(inferenceId, taskType).setInput(List.of(text)).build(), pvcs);
    }

    /**
     * Factory for creating {@link TextEmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, TaskType taskType, ExpressionEvaluator textEvaluator)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new TextEmbeddingRequestIterator(inferenceId, (BytesRefBlock) textEvaluator.eval(inputPage));
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
