/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

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
                inferenceRunner.threadPool(),
                inferenceId,
                queryText,
                rowEncoderFactory().get(driverContext),
                scoreChannel
            );
        }
    }

    private static final int DEFAULT_BATCH_SIZE = 20;
    private final String queryText;
    private final ExpressionEvaluator rowEncoder;
    private final int scoreChannel;

    // TODO: make it configurable either in the command or as query pragmas
    private final int batchSize = DEFAULT_BATCH_SIZE;

    public RerankOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        ThreadPool threadPool,
        String inferenceId,
        String queryText,
        ExpressionEvaluator rowEncoder,
        int scoreChannel
    ) {
        super(driverContext, inferenceRunner, threadPool, inferenceId);
        this.queryText = queryText;
        this.rowEncoder = rowEncoder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void doClose() {
        Releasables.close(rowEncoder);
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    @Override
    protected RerankOperatorRequestIterator requests(Page inputPage) {
        return new RerankOperatorRequestIterator((BytesRefBlock) rowEncoder.eval(inputPage), inferenceId(), queryText, batchSize);
    }

    @Override
    protected RerankOperatorOutputBuilder outputBuilder(Page inputPage) {
        try {
            return new RerankOperatorOutputBuilder(
                blockFactory().newDoubleBlockBuilder(inputPage.getPositionCount()),
                inputPage,
                scoreChannel
            );
        } catch (Exception e) {
            releasePageOnAnyThread(inputPage);
            throw (e);
        }
    }
}
