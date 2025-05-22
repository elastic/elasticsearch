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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

public class RerankOperator extends AsyncOperator<RerankOperator.OngoingRerank> {

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
    protected void performAsync(Page inputPage, ActionListener<OngoingRerank> listener) {
        // Ensure input page blocks are released when the listener is called.
        listener = listener.delegateResponse((l, e) -> {
            releasePageOnAnyThread(inputPage);
            l.onFailure(e);
        });
        try {
            inferenceRunner.doInference(buildInferenceRequest(inputPage), listener.map(resp -> new OngoingRerank(inputPage, resp)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(rowEncoder);
    }

    @Override
    protected void releaseFetchedOnAnyThread(OngoingRerank result) {
        releasePageOnAnyThread(result.inputPage);
    }

    @Override
    public Page getOutput() {
        var fetched = fetchFromBuffer();
        if (fetched == null) {
            return null;
        } else {
            return fetched.buildOutput(blockFactory, scoreChannel);
        }
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
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

    public static final class OngoingRerank {
        final Page inputPage;
        final Double[] rankedScores;

        OngoingRerank(Page inputPage, InferenceAction.Response resp) {
            if (resp.getResults() instanceof RankedDocsResults == false) {
                releasePageOnAnyThread(inputPage);
                throw new IllegalStateException(
                    "Inference result has wrong type. Got ["
                        + resp.getResults().getClass()
                        + "] while expecting ["
                        + RankedDocsResults.class
                        + "]"
                );

            }
            final var results = (RankedDocsResults) resp.getResults();
            this.inputPage = inputPage;
            this.rankedScores = extractRankedScores(inputPage.getPositionCount(), results);
        }

        private static Double[] extractRankedScores(int positionCount, RankedDocsResults rankedDocsResults) {
            Double[] sortedRankedDocsScores = new Double[positionCount];
            for (RankedDocsResults.RankedDoc rankedDoc : rankedDocsResults.getRankedDocs()) {
                sortedRankedDocsScores[rankedDoc.index()] = (double) rankedDoc.relevanceScore();
            }
            return sortedRankedDocsScores;
        }

        Page buildOutput(BlockFactory blockFactory, int scoreChannel) {
            int blockCount = Integer.max(inputPage.getBlockCount(), scoreChannel + 1);
            Block[] blocks = new Block[blockCount];
            Page outputPage = null;
            try (Releasable ignored = inputPage::releaseBlocks) {
                for (int b = 0; b < blockCount; b++) {
                    if (b == scoreChannel) {
                        blocks[b] = buildScoreBlock(blockFactory);
                    } else {
                        blocks[b] = inputPage.getBlock(b);
                        blocks[b].incRef();
                    }
                }
                outputPage = new Page(blocks);
                return outputPage;
            } finally {
                if (outputPage == null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        }

        private Block buildScoreBlock(BlockFactory blockFactory) {
            try (DoubleBlock.Builder scoreBlockFactory = blockFactory.newDoubleBlockBuilder(rankedScores.length)) {
                for (Double rankedScore : rankedScores) {
                    if (rankedScore != null) {
                        scoreBlockFactory.appendDouble(rankedScore);
                    } else {
                        scoreBlockFactory.appendNull();
                    }
                }
                return scoreBlockFactory.build();
            }
        }
    }
}
