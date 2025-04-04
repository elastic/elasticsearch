/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

public class RerankOperator extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

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

    private final InferenceRunner inferenceRunner;
    private final BlockFactory blockFactory;
    private final String inferenceId;
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
        super(driverContext, MAX_INFERENCE_WORKER);

        assert inferenceRunner.getThreadContext() != null;

        this.blockFactory = driverContext.blockFactory();
        this.inferenceRunner = inferenceRunner;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.rowEncoder = rowEncoder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        // Ensure input page blocks are released when the listener is called.
        final ActionListener<Page> outputListener = ActionListener.runAfter(listener, () -> { releasePageOnAnyThread(inputPage); });

        try {
            inferenceRunner.doInference(
                buildInferenceRequest(inputPage),
                ActionListener.wrap(
                    inferenceResponse -> outputListener.onResponse(buildOutput(inputPage, inferenceResponse)),
                    outputListener::onFailure
                )
            );
        } catch (Exception e) {
            outputListener.onFailure(e);
        }
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(rowEncoder);
    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            return buildOutput(inputPage, rankedDocsResults);

        }

        throw new IllegalStateException(
            "Inference result has wrong type. Got ["
                + inferenceResponse.getResults().getClass()
                + "] while expecting ["
                + RankedDocsResults.class
                + "]"
        );
    }

    private Page buildOutput(Page inputPage, RankedDocsResults rankedDocsResults) {
        int blockCount = Integer.max(inputPage.getBlockCount(), scoreChannel + 1);
        Block[] blocks = new Block[blockCount];

        try {
            for (int b = 0; b < blockCount; b++) {
                if (b == scoreChannel) {
                    blocks[b] = buildScoreBlock(inputPage, rankedDocsResults);
                } else {
                    blocks[b] = inputPage.getBlock(b);
                    blocks[b].incRef();
                }
            }
            return new Page(blocks);
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw (e);
        }
    }

    private Block buildScoreBlock(Page inputPage, RankedDocsResults rankedDocsResults) {
        Double[] sortedRankedDocsScores = new Double[inputPage.getPositionCount()];

        try (DoubleBlock.Builder scoreBlockFactory = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())) {
            for (RankedDocsResults.RankedDoc rankedDoc : rankedDocsResults.getRankedDocs()) {
                sortedRankedDocsScores[rankedDoc.index()] = (double) rankedDoc.relevanceScore();
            }

            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                if (sortedRankedDocsScores[pos] != null) {
                    scoreBlockFactory.appendDouble(sortedRankedDocsScores[pos]);
                } else {
                    scoreBlockFactory.appendNull();
                }
            }

            return scoreBlockFactory.build();
        }
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) {
        try (BytesRefBlock encodedRowsBlock = (BytesRefBlock) rowEncoder.eval(inputPage)) {
            assert (encodedRowsBlock.getPositionCount() == inputPage.getPositionCount());
            String[] inputs = new String[inputPage.getPositionCount()];
            BytesRef buffer = new BytesRef();

            for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
                if (encodedRowsBlock.isNull(pos)) {
                    inputs[pos] = "";
                } else {
                    buffer = encodedRowsBlock.getBytesRef(encodedRowsBlock.getFirstValueIndex(pos), buffer);
                    inputs[pos] = BytesRefs.toString(buffer);
                }
            }

            return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputs)).setQuery(queryText).build();
        }
    }
}
