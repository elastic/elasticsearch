/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.esql.inference.AbstractEmbeddingRequestIterator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Embedding request iterator for typed (DataType) inputs.
 * <p>
 * Produces {@link EmbeddingAction.Request} items with typed content via {@link InferenceStringGroup}
 * </p>
 */
class EmbeddingRequestIterator extends AbstractEmbeddingRequestIterator {

    private final DataType dataType;
    private final TimeValue timeout;

    EmbeddingRequestIterator(String inferenceId, BytesRefBlock textBlock, DataType dataType, TimeValue timeout) {
        super(inferenceId, TaskType.EMBEDDING, textBlock);
        this.dataType = dataType;
        this.timeout = timeout;
    }

    @Override
    protected BulkInferenceRequestItem buildRequestItem(String text, PositionValueCountsBuilder pvcs) {
        if (text == null) {
            return new BulkInferenceRequestItem(null, pvcs);
        }
        InferenceString inferenceString = new InferenceString(dataType, text);
        EmbeddingRequest embeddingRequest = new EmbeddingRequest(
            List.of(new InferenceStringGroup(inferenceString)),
            InputType.UNSPECIFIED,
            Map.of()
        );
        return new BulkInferenceRequestItem(
            new EmbeddingAction.Request(
                inferenceId,
                taskType,
                embeddingRequest,
                Objects.requireNonNullElse(timeout, BaseInferenceActionRequest.getDefaultTimeoutForTaskType(TaskType.TEXT_EMBEDDING))
            ),
            pvcs
        );
    }

    /**
     * Factory for creating {@link EmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, TaskType taskType, ExpressionEvaluator textEvaluator, DataType dataType, TimeValue timeout)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new EmbeddingRequestIterator(inferenceId, (BytesRefBlock) textEvaluator.eval(inputPage), dataType, timeout);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
