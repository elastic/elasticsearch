/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

public class RerankOperator extends AsyncOperator<Page> {

    private static final Logger logger = LogManager.getLogger(RerankOperator.class);

    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        EvalOperator.ExpressionEvaluator.Factory inputEvaluatorFactory,
        int scoreChannel,
        int maxOutstandingRequests
    ) implements OperatorFactory {
        @Override
        public RerankOperator get(DriverContext driverContext) {
            return new RerankOperator(
                inferenceService,
                inferenceId,
                queryText,
                inputEvaluatorFactory.get(driverContext),
                scoreChannel,
                driverContext,
                maxOutstandingRequests
            );
        }

        @Override
        public String describe() {
            return "RerankOperator[maxOutstandingRequests = " + maxOutstandingRequests + "]";
        }
    }

    private final InferenceService inferenceService;
    private final BlockFactory blockFactory;
    private final String inferenceId;
    private final String queryText;
    private final EvalOperator.ExpressionEvaluator inputEvaluator;
    private final int scoreChannel;

    public RerankOperator(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        EvalOperator.ExpressionEvaluator inputEvaluator,
        int scoreChannel,
        DriverContext driverContext,
        int maxOutstandingRequests
    ) {
        super(driverContext, maxOutstandingRequests);
        this.inferenceService = inferenceService;
        this.blockFactory = driverContext.blockFactory();
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.inputEvaluator = inputEvaluator;
        this.scoreChannel = scoreChannel;
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        logger.debug(
            "Reranking operator called with inferenceId=[{}], queryText=[{}] with page of size [{}]",
            inferenceId,
            queryText,
            inputPage.getPositionCount()
        );
        inferenceService.infer(buildInferenceRequest(inputPage), ActionListener.wrap((inferenceResponse) -> {
            listener.onResponse(buildOutput(inputPage, inferenceResponse));
        }, listener::onFailure));
    }

    @Override
    protected void doClose() {

    }

    private void performInference(Page inputPage, ActionListener<Page> listener) {
        // Moved to the InferenceOperator?
        inferenceService.infer(buildInferenceRequest(inputPage), ActionListener.wrap((inferenceResponse) -> {
            listener.onResponse(buildOutput(inputPage, inferenceResponse));
        }, listener::onFailure));
    }

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        logger.warn("Result {}", inferenceResponse.getResults().asMap().get("rerank"));
        logger.warn("Inference response [{}]", inferenceResponse);
        logger.warn("Score channel {}", scoreChannel);

        int blockCount = inputPage.getBlockCount();
        Block.Builder[] blocksBuilders = new Block.Builder[blockCount];

        for (int b = 0; b < blockCount; b++) {
            if (b == scoreChannel) {
                blocksBuilders[b] = ElementType.DOUBLE.newBlockBuilder(inputPage.getPositionCount(), blockFactory);
            } else {
                blocksBuilders[b] = inputPage.getBlock(b).elementType().newBlockBuilder(inputPage.getPositionCount(), blockFactory);
            }
        }

        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            for (var rankedDoc : rankedDocsResults.getRankedDocs()) {
                for (int b = 0; b < blockCount; b++) {
                    if (b == scoreChannel) {
                        if (blocksBuilders[b] instanceof DoubleBlock.Builder scoreBlockBuilder) {
                            scoreBlockBuilder.beginPositionEntry().appendDouble(rankedDoc.relevanceScore()).endPositionEntry();
                        }
                    } else {
                        blocksBuilders[b].copyFrom(inputPage.getBlock(b), rankedDoc.index(), rankedDoc.index() + 1);
                    }
                }
            }

            return new Page(Block.Builder.buildAll(blocksBuilders));
        }

        throw new IllegalStateException(
            "Inference result has wrong type. Got ["
                + inferenceResponse.getResults().getClass()
                + "] while expecting ["
                + RankedDocsResults.class
                + "]"
        );
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) {
        BytesRef scratch = new BytesRef();
        String[] inputs = new String[inputPage.getPositionCount()];
        BytesRefBlock inputBlock = (BytesRefBlock) inputEvaluator.eval(inputPage);

        for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
            if (inputBlock.isNull(pos) || inputBlock.getValueCount(pos) > 1) {
                inputs[pos] = "";
            } else {
                inputs[pos] = inputBlock.getBytesRef(pos, scratch).utf8ToString();
            }
        }

        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputs)).setQuery(queryText).build();
    }
}
