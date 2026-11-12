/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
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
 * that must be distributed back to their corresponding input positions based on the response's position value counts array.
 * </p>
 * <p>
 * <b>Multi-valued Position Handling:</b> When a position contributes multiple documents (multi-valued field),
 * the scores are aggregated using a max aggregation strategy. This ensures each position gets a single
 * relevance score in the output block.
 * </p>
 * <p>
 * <b>Position Value Counts Semantics:</b> The position value counts array indicates how many documents each position contributed.
 * For example, [1, 0, 2, 1] means:
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
     * The position value counts in each response determine how output scores are distributed across positions:
     * <ul>
     *   <li>positionValueCounts[i] = 0: produces a null value for position i (null/empty input)</li>
     *   <li>positionValueCounts[i] = 1: produces one relevance score for position i</li>
     *   <li>positionValueCounts[i] = N (N > 1): produces the max of N relevance scores for position i (multi-valued field)</li>
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
            return withScoreBlock(inputPage, scoreBlockBuilder.build());
        }
    }

    /**
     * Builds the output page from scores that cover more documents than the page's own, for example the scores of one
     * request sent for several pages. The page's documents are the ones starting at {@code firstScore}, and
     * {@code positionValueCounts} distributes their scores across positions as in {@link #buildOutputPage(Page, List)}.
     *
     * @param inputPage           The original input page
     * @param scores              Relevance scores in document order, as returned by {@link #scores}
     * @param firstScore          The index in {@code scores} of the page's first document
     * @param positionValueCounts The number of documents each position of the page contributed
     * @return A new page with the relevance scores at the score channel position
     */
    Page buildOutputPage(Page inputPage, double[] scores, int firstScore, int[] positionValueCounts) {
        try (DoubleBlock.Builder scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount())) {
            appendScores(scoreBlockBuilder, scores, firstScore, positionValueCounts);
            return withScoreBlock(inputPage, scoreBlockBuilder.build());
        }
    }

    /**
     * Puts {@code scoreBlock} at the score channel of {@code inputPage}, replacing the block there if there is one.
     */
    private Page withScoreBlock(Page inputPage, DoubleBlock scoreBlock) {
        // Append the score block to the input page
        Page outputPage = inputPage.appendBlock(scoreBlock);

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

    /**
     * Appends scores from a single inference response to the score block.
     * <p>
     * The response's position value counts determine how scores are distributed:
     * <ul>
     *   <li>For each position with value count 0: appends a null score</li>
     *   <li>For each position with value count 1: appends the corresponding score</li>
     *   <li>For each position with value count N > 1: appends the max of N consecutive scores</li>
     * </ul>
     * <p>
     * Scores are sorted by their index before processing to ensure correct alignment with positions.
     *
     * @param builder  The block builder to append scores to
     * @param response The inference response item containing the reranking results and position value counts
     * @throws IllegalStateException if the number of scores doesn't match the sum of position value counts
     */
    private void appendResponseToBlock(DoubleBlock.Builder builder, BulkInferenceResponseItem response) {
        // Handle null responses or null position value counts
        if (response == null || response.positionValueCounts() == null) {
            return;
        }

        double[] scores = extractRerankScores(response);

        // Validate that the number of scores matches the expected count from the position value counts
        int expectedScoreCount = IntStream.of(response.positionValueCounts()).sum();
        if (scores.length != expectedScoreCount) {
            throw new IllegalStateException(
                format(
                    "Mismatch between score count and position value counts: expected {} scores but got {}",
                    expectedScoreCount,
                    scores.length
                )
            );
        }

        appendScores(builder, scores, 0, response.positionValueCounts());
    }

    /**
     * Appends one value per position to the score block: null for a position with no documents, otherwise the max of the
     * scores of its documents, which are the next ones in {@code scores}, starting at {@code firstScore}.
     */
    private static void appendScores(DoubleBlock.Builder builder, double[] scores, int firstScore, int[] positionValueCounts) {
        var currentScoreIndex = firstScore;

        for (int valueCount : positionValueCounts) {
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
     * Extracts and sorts relevance scores from a rerank inference response item.
     *
     * @param responseItem The inference response item to extract scores from
     * @return An array of relevance scores sorted by document index, or an empty array if response is null
     */
    private static double[] extractRerankScores(BulkInferenceResponseItem responseItem) {
        if (responseItem == null || responseItem.inferenceResponse() == null) {
            return new double[0];
        }

        return scores(responseItem.inferenceResponse());
    }

    /**
     * Extracts and sorts relevance scores from a rerank inference response.
     * <p>
     * Scores are sorted by their document index to ensure they align with the order
     * documents were sent in the request. This is critical for multi-valued positions
     * and maintaining correct position-to-score mapping.
     * </p>
     *
     * @param response The inference response to extract scores from
     * @return An array of relevance scores sorted by document index
     * @throws IllegalStateException if the results are not of type {@link RankedDocsResults}
     */
    static double[] scores(InferenceAction.Response response) {
        return extractRankedDocsResults(response).getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .mapToDouble(RankedDocsResults.RankedDoc::relevanceScore)
            .toArray();
    }

    /**
     * Extracts {@link RankedDocsResults} from an inference response.
     *
     * @param response The inference response to extract from
     * @return The ranked docs results
     * @throws IllegalStateException if the results are not of type {@link RankedDocsResults}
     */
    private static RankedDocsResults extractRankedDocsResults(InferenceAction.Response response) {
        if (response.getResults() instanceof RankedDocsResults rankedDocsResults) {
            return rankedDocsResults;
        }

        // Strings.format, not LoggerMessageFormat.format: with a String as the first argument, the latter resolves to its
        // (prefix, messagePattern, args) overload and treats the class name as the pattern.
        throw new IllegalStateException(
            Strings.format(
                "Inference result has wrong type. Got [%s] while expecting [%s]",
                response.getResults().getClass().getName(),
                RankedDocsResults.class.getName()
            )
        );
    }
}
