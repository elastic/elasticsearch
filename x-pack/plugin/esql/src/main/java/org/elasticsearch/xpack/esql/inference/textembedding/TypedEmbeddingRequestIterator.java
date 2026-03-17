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
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.List;
import java.util.Map;

/**
 * Embedding request iterator for typed (DataType/DataFormat) inputs.
 * <p>
 * Produces {@link EmbeddingAction.Request} items with typed content via {@link InferenceStringGroup}
 * and binds {@link org.elasticsearch.xpack.esql.inference.InferenceService#executeEmbeddingInference}
 * as the executor.
 * </p>
 */
class TypedEmbeddingRequestIterator extends AbstractEmbeddingRequestIterator {

    private final DataType dataType;
    private final DataFormat dataFormat;

    TypedEmbeddingRequestIterator(
        String inferenceId,
        TaskType taskType,
        BytesRefBlock textBlock,
        DataType dataType,
        DataFormat dataFormat
    ) {
        super(inferenceId, taskType, textBlock);
        this.dataType = dataType;
        this.dataFormat = dataFormat;
    }

    @Override
    protected BulkInferenceRequestItem buildRequestItem(String text, PositionValueCountsBuilder pvcs) {
        if (text == null) {
            return new BulkInferenceRequestItem(null, null, pvcs);
        }
        InferenceString inferenceString = dataFormat != null
            ? new InferenceString(dataType, dataFormat, text)
            : new InferenceString(dataType, text);
        EmbeddingRequest embeddingRequest = new EmbeddingRequest(
            List.of(new InferenceStringGroup(inferenceString)),
            InputType.UNSPECIFIED,
            Map.of()
        );
        EmbeddingAction.Request req = new EmbeddingAction.Request(
            inferenceId,
            taskType,
            embeddingRequest,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        return new BulkInferenceRequestItem(req, (svc, lst) -> svc.executeEmbeddingInference(req, lst), pvcs);
    }

    /**
     * Factory for creating {@link TypedEmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, TaskType taskType, ExpressionEvaluator textEvaluator, DataType dataType, DataFormat dataFormat)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new TypedEmbeddingRequestIterator(
                inferenceId,
                taskType,
                (BytesRefBlock) textEvaluator.eval(inputPage),
                dataType,
                dataFormat
            );
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
