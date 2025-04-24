/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

public class RerankOperator extends InferenceOperator<RankedDocsResults> {
    public record Factory(
        InferenceRunner inferenceRunner,
        String inferenceId,
        String queryText,
        ExpressionEvaluator.Factory rowEncoderFactory,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceRunner,
                inferenceId,
                queryText,
                rowEncoderFactory().get(driverContext),
                scoreChannel
            );
        }
    }

    private final BlockFactory blockFactory;
    private final String queryText;
    private final ExpressionEvaluator rowEncoder;
    private final int scoreChannel;

    public RerankOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        String queryText,
        ExpressionEvaluator rowEncoder,
        int scoreChannel
    ) {
        super(driverContext, inferenceRunner, inferenceId);

        this.blockFactory = driverContext.blockFactory();
        this.queryText = queryText;
        this.rowEncoder = rowEncoder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(rowEncoder);
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    @Override
    protected TaskType taskType() {
        return TaskType.RERANK;
    }

    @Override
    protected BulkInferenceRequestIterator bulkInferenceRequestIterator(Page inputPage) {
        return new RerankBulkInferenceRequestRequestIterator((BytesRefBlock) rowEncoder.eval(inputPage), this::inferenceRequestBuilder);
    }

    @Override
    protected InferenceAction.Request.Builder inferenceRequestBuilder() {
        return super.inferenceRequestBuilder().setQuery(queryText);
    }

    @Override
    protected BulkInferenceOutputBuilder<RankedDocsResults, Page> bulkOutputBuilder(Page inputPage) {
        return new RerankBulkInferenceOutputBuilder(blockFactory, inputPage, scoreChannel);
    }
}
