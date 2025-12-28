/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceResponse;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Builds the output page for the {@link RerankOperator} by adding
 *  * reranked relevance scores into the specified score channel of the input page.
 */

class RerankOperatorOutputBuilder implements InferenceOperator.OutputBuilder {

    private final Page inputPage;
    private final DoubleBlock.Builder scoreBlockBuilder;
    private final int scoreChannel;

    RerankOperatorOutputBuilder(DoubleBlock.Builder scoreBlockBuilder, Page inputPage, int scoreChannel) {
        this.inputPage = inputPage;
        this.scoreBlockBuilder = scoreBlockBuilder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    public void close() {
        Releasables.close(scoreBlockBuilder);
    }

    /**
     * Constructs a new output {@link Page} which contains all original blocks from the input page, with the reranked scores
     * inserted at {@code scoreChannel}.
     */
    @Override
    public Page buildOutput() {
        Page outputPage = inputPage.appendBlock(scoreBlockBuilder.build());

        if (scoreChannel == inputPage.getBlockCount()) {
            // Just need to append the block at the end
            // We can just return the output page we have just created
            return outputPage;
        }

        try {
            // We need to project the last column to the score channel.
            int[] blockNapping = IntStream.range(0, inputPage.getBlockCount())
                .map(channel -> channel == scoreChannel ? inputPage.getBlockCount() : channel)
                .toArray();

            return outputPage.projectBlocks(blockNapping);
        } finally {
            // Releasing the output page since projection is incrementing block references.
            releasePageOnAnyThread(outputPage);
        }
    }

    @Override
    public void addInferenceResponse(BulkInferenceResponse bulkInferenceResponse) {
        List<RankedDocsResults.RankedDoc> rankedDocs = inferenceResults(bulkInferenceResponse.response());
        int currentIndex = 0;
        for (int valueCount : bulkInferenceResponse.shape()) {
            if (valueCount == 0) {
                scoreBlockBuilder.appendNull();
                continue;
            }

            // Extract scores for this position and find the max
            double maxScore = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < valueCount; i++) {
                RankedDocsResults.RankedDoc doc = rankedDocs.get(currentIndex++);
                maxScore = Math.max(maxScore, doc.relevanceScore());
            }
            scoreBlockBuilder.appendDouble(maxScore);
        }
    }

    private List<RankedDocsResults.RankedDoc> inferenceResults(InferenceAction.Response inferenceResponse) {
        if (inferenceResponse == null) {
            return List.of();
        }

        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, RankedDocsResults.class)
            .getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .toList();
    }
}
