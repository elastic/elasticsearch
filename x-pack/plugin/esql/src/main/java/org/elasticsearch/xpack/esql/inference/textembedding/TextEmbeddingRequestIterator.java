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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.AbstractEmbeddingRequestIterator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.List;

import static org.elasticsearch.xpack.esql.inference.InferenceService.ESQL_PRODUCT_USE_CASE;

/**
 * Embedding request iterator for plain (untyped) text inputs.
 * <p>
 * Produces {@link InferenceAction.Request} items
 * </p>
 */
class TextEmbeddingRequestIterator extends AbstractEmbeddingRequestIterator {

    private final TimeValue timeout;

    TextEmbeddingRequestIterator(String inferenceId, BytesRefBlock textBlock, int batchSize, TimeValue timeout) {
        super(inferenceId, TaskType.TEXT_EMBEDDING, textBlock, batchSize);
        this.timeout = timeout;
    }

    @Override
    protected BulkInferenceRequestItem buildRequestItem(List<String> texts, PositionValueCountsBuilder pvcs) {
        if (texts.isEmpty()) {
            return new BulkInferenceRequestItem(null, pvcs);
        }
        InferenceAction.Request.Builder builder = InferenceAction.Request.builder(inferenceId, taskType)
            .setInput(texts)
            .setContext(new InferenceContext(ESQL_PRODUCT_USE_CASE));
        if (timeout != null) {
            builder.setInferenceTimeout(timeout);
        }
        return new BulkInferenceRequestItem(builder.build(), pvcs);
    }

    /**
     * Factory for creating {@link TextEmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, TaskType taskType, ExpressionEvaluator textEvaluator, int batchSize, TimeValue timeout)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new TextEmbeddingRequestIterator(inferenceId, (BytesRefBlock) textEvaluator.eval(inputPage), batchSize, timeout);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
