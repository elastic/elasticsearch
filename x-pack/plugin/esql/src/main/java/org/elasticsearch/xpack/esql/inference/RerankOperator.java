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

public class RerankOperator extends AsyncOperator {

    private static final Logger logger = LogManager.getLogger(RerankOperator.class);

    public record Factory(InferenceService inferenceService, String inferenceId, String queryText, EvalOperator.ExpressionEvaluator.Factory inputEvaluatorFactory, int windowSize, int maxOutstandingRequests) implements OperatorFactory {
        @Override
        public RerankOperator get(DriverContext driverContext) {
            return new RerankOperator(inferenceService, inferenceId, queryText, inputEvaluatorFactory.get(driverContext), windowSize, driverContext, maxOutstandingRequests);
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
    private final int windowSize;
    private Page firstPage;
    private boolean firstPageSent = false;


    public RerankOperator(InferenceService inferenceService, String inferenceId, String queryText, EvalOperator.ExpressionEvaluator inputEvaluator, int windowSize, DriverContext driverContext, int maxOutstandingRequests) {
        super(driverContext, maxOutstandingRequests);
        this.inferenceService = inferenceService;
        this.blockFactory = driverContext.blockFactory();
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.inputEvaluator = inputEvaluator;
        this.windowSize = windowSize;
    }

    @Override
    public void addInput(Page input) {
        if (firstPage == null) {
            firstPage = input;
        } else if (firstPage.getPositionCount() < windowSize) {
            firstPage = mergePages(firstPage, input);
            if (firstPage.getPositionCount() >= windowSize) {
                firstPageSent = true;
                super.addInput(firstPage);
            }
        } else {
            super.addInput(input);
        }
    }


    private Page mergePages(Page first, Page second) {
        assert first.getBlockCount() == second.getBlockCount();

        int blockCount = first.getBlockCount();;
        Block[] blocks = new Block[blockCount];

        int totalPositionCount = first.getPositionCount() + second.getPositionCount();

        for (int b = 0; b < blockCount; b++) {
            Block.Builder blockBuilder = first.getBlock(b).elementType().newBlockBuilder(totalPositionCount, blockFactory);
            blockBuilder.copyFrom(first.getBlock(b), 0, first.getPositionCount());
            blockBuilder.copyFrom(second.getBlock(b), 0, second.getPositionCount());
            blocks[b] = blockBuilder.build();
        }

        first.releaseBlocks();
        second.releaseBlocks();

        return new Page(blocks);
    }

    @Override
    public void finish() {
        if (firstPage != null && firstPageSent == false) {
            firstPageSent = true;
            super.addInput(firstPage);
        }

        super.finish();
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        logger.debug("Reranking operator called with inferenceId=[{}], queryText=[{}] and windowSize=[{}]", inferenceId, queryText, windowSize);
        if (inputPage == firstPage) {
            performInference(inputPage, listener);
        } else {
            listener.onResponse(inputPage);
        }
    }

    @Override
    protected void doClose() {
        if (firstPage != null) {
            firstPage.releaseBlocks();
        }
    }

    private void performInference(Page inputPage, ActionListener<Page> listener) {
        // Moved to the InferenceOperator?
        inferenceService.infer(buildInferenceRequest(inputPage), ActionListener.wrap(
            (inferenceResponse) -> { listener.onResponse(buildOutput(inputPage, inferenceResponse));}, listener::onFailure));
    }

    private Page buildOutput(Page inputPage, InferenceAction.Response inferenceResponse) {
        logger.warn("Result {}", inferenceResponse.getResults().asMap().get("rerank"));
        logger.warn("Inference response [{}]", inferenceResponse);

        int blockCount = inputPage.getBlockCount();
        Block.Builder[] blocksBuilders = new Block.Builder[blockCount];

        for (int b = 0; b < blockCount; b++) {
            blocksBuilders[b] = inputPage.getBlock(b).elementType().newBlockBuilder(inputPage.getPositionCount(), blockFactory);
        }

        if (inferenceResponse.getResults() instanceof RankedDocsResults rankedDocsResults) {
            for (var rankedDoc: rankedDocsResults.getRankedDocs()) {
                for (int b = 0; b < blockCount; b++) {
                    blocksBuilders[b].copyFrom(inputPage.getBlock(b), rankedDoc.index(), rankedDoc.index() + 1);
                }
            }

            if (inputPage.getPositionCount() > windowSize) {
                for (int b = 0; b < blockCount; b++) {
                    blocksBuilders[b].copyFrom(inputPage.getBlock(b), windowSize, inputPage.getPositionCount());
                }
            }


            return new Page(Block.Builder.buildAll(blocksBuilders));
        }

        throw new IllegalStateException("Inference result has wrong type. Got [" + inferenceResponse.getResults().getClass() + "] while expecting [" + RankedDocsResults.class + "]");
    }

    private InferenceAction.Request buildInferenceRequest(Page inputPage) {
        // To be an abstract method of InferenceOperator?

        // 1. Truncate the page to the windowSize
        inputPage = head(inputPage);

        // 2. Input block
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

        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK)
            .setInput(List.of(inputs))
            .setQuery(queryText)
            .build();
    }

    private Page head(Page inputPage) {
        if (inputPage.getPositionCount() <= windowSize) {
            return inputPage;
        }

        int[] filter = new int[windowSize];
        for (int i = 0; i < windowSize; i++) {
            filter[i] = i;
        }
        Block[] blocks = new Block[inputPage.getBlockCount()];

        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = inputPage.getBlock(b).filter(filter);
        }

        return new Page(blocks);
    }
}
