/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.OutputBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Builds output pages for rerank inference operations.
 * <p>
 * Converts {@link RankedDocsResults} from inference responses into a {@link DoubleBlock} containing
 * relevance scores. The score block replaces the block at the specified channel position in the output page.
 * The output page will have the same number of blocks as the input page, with the score block at
 * the {@code scoreChannel} position.
 * </p>
 * <p>
 * Since reranking operates on batches of documents, each inference response contains multiple scores
 * that must be distributed back to their corresponding input positions based on the response shape array.
 * </p>
 * <p>
 * <b>Multi-valued Position Handling:</b> When a position contributes multiple documents (multi-valued field),
 * the scores are aggregated using a max aggregation strategy. This ensures each position gets a single
 * relevance score in the output block.
 * </p>
 * <p>
 * <b>Shape Array Semantics:</b> The shape array indicates how many documents each position contributed.
 * For example, shape [1, 0, 2, 1] means:
 * <ul>
 *   <li>Position 0: 1 document → receives score at index 0</li>
 *   <li>Position 1: 0 documents (null/empty) → receives null</li>
 *   <li>Position 2: 2 documents → receives max(score[1], score[2])</li>
 *   <li>Position 3: 1 document → receives score at index 3</li>
 * </ul>
 */
class RerankOutputBuilder implements OutputBuilder {

    private final BlockFactory blockFactory;
    private final int scoreChannel;

    /**
     * Constructs a new output builder.
     *
     * @param blockFactory The block factory for creating output blocks.
     * @param scoreChannel The channel position where the score block should be inserted.
     */
    RerankOutputBuilder(BlockFactory blockFactory, int scoreChannel) {
        this.blockFactory = blockFactory;
        this.scoreChannel = scoreChannel;
    }

    /**
     * Builds the output page by converting inference responses into a {@link DoubleBlock} of scores.
     * <p>
     * The shape array in each response determines how output scores are distributed across positions:
     * <ul>
     *   <li>shape[i] = 0: produces a null value for position i (null/empty input)</li>
     *   <li>shape[i] = 1: produces one relevance score for position i</li>
     *   <li>shape[i] = N (N > 1): produces the max of N relevance scores for position i (multi-valued field)</li>
     * </ul>
     * The score block replaces the block at the {@code scoreChannel} position in the output page.
     * The output page has the same number of blocks as the input page.
     *
     * @param inputPage The original input page
     * @param responses The ordered list of inference responses corresponding to batches of input positions
     * @return A new page with the relevance scores at the score channel position
     */
    @Override
    public Page buildOutputPage(Page inputPage, List<BulkInferenceResponseItem> responses) {
        int positionCount = inputPage.getPositionCount();

        try (DoubleBlock.Builder scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (BulkInferenceResponseItem response : responses) {
                appendResponseToBlock(scoreBlockBuilder, response);
            }

            // Append the score block to the input page
            Page outputPage = inputPage.appendBlock(scoreBlockBuilder.build());

            // If scoreChannel is at the end, we're done
            if (scoreChannel == inputPage.getBlockCount()) {
                return outputPage;
            }

            // Otherwise, project the blocks to replace the score channel with the score block
            try {
                // We need to project the last column to the score channel, replacing what was there.
                int[] blockMapping = IntStream.range(0, inputPage.getBlockCount())
                    .map(channel -> channel == scoreChannel ? inputPage.getBlockCount() : channel)
                    .toArray();

                return outputPage.projectBlocks(blockMapping);
            } finally {
                // Release the temporary output page since projection increments block references
                outputPage.allowPassingToDifferentDriver();
                outputPage.releaseBlocks();
            }
        }
    }

    /**
     * Appends scores from a single inference response to the score block.
     * <p>
     * The response shape array determines how scores are distributed:
     * <ul>
     *   <li>For each position with shape value 0: appends a null score</li>
     *   <li>For each position with shape value 1: appends the corresponding score</li>
     *   <li>For each position with shape value N > 1: appends the max of N consecutive scores</li>
     * </ul>
     * <p>
     * Scores are sorted by their index before processing to ensure correct alignment with positions.
     *
     * @param builder  The block builder to append scores to
     * @param response The inference response item containing the reranking results and shape information
     * @throws IllegalStateException if the number of scores doesn't match the sum of shape values
     */
    private void appendResponseToBlock(DoubleBlock.Builder builder, BulkInferenceResponseItem response) {
        // Handle null responses or null shape
        if (response == null || response.shape() == null) {
            return;
        }

        double[] scores = extractRerankScores(response);

        // Validate that the number of scores matches the expected count from the shape
        int expectedScoreCount = IntStream.of(response.shape()).sum();
        if (scores.length != expectedScoreCount) {
            throw new IllegalStateException(
                format("Mismatch between score count and shape: expected {} scores but got {}", expectedScoreCount, scores.length)
            );
        }

        var currentScoreIndex = 0;

        for (int valueCount : response.shape()) {
            if (valueCount == 0) {
                // No scores for this position, append a null value to the block
                builder.appendNull();
                continue;
            }

            // Compute the aggregated score for this position
            // TODO: use an aggregator model, instead of just computing the max score.
            double maxScore = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < valueCount; i++) {
                maxScore = Math.max(maxScore, scores[currentScoreIndex++]);
            }

            builder.appendDouble(maxScore);
        }
    }

    /**
     * Extracts and sorts relevance scores from a rerank inference response.
     * <p>
     * Scores are sorted by their document index to ensure they align with the order
     * documents were sent in the request. This is critical for multi-valued positions
     * and maintaining correct position-to-score mapping.
     * </p>
     *
     * @param responseItem The inference response item to extract scores from
     * @return An array of relevance scores sorted by document index, or an empty array if response is null
     */
    private double[] extractRerankScores(BulkInferenceResponseItem responseItem) {
        if (responseItem == null || responseItem.inferenceResponse() == null) {
            return new double[0];
        }

        return extractRankedDocsResults(responseItem).getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .mapToDouble(RankedDocsResults.RankedDoc::relevanceScore)
            .toArray();
    }

    /**
     * Extracts {@link RankedDocsResults} from an inference response.
     *
     * @param responseItem The inference response item to extract from
     * @return The ranked docs results
     * @throws IllegalStateException if the response is null, has no results, or is not of type {@link RankedDocsResults}
     */
    private RankedDocsResults extractRankedDocsResults(BulkInferenceResponseItem responseItem) {
        if (responseItem == null || responseItem.inferenceResponse() == null) {
            throw new IllegalStateException("Inference response is null");
        }

        if (responseItem.inferenceResponse().getResults() instanceof RankedDocsResults rankedDocsResults) {
            return rankedDocsResults;
        }

        throw new IllegalStateException(
            format(
                "Inference result has wrong type. Got [{}] while expecting [{}]",
                responseItem.inferenceResponse().getResults().getClass().getName(),
                RankedDocsResults.class.getName()
            )
        );
    }
}
