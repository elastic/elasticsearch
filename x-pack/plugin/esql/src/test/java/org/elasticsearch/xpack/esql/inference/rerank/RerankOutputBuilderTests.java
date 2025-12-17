/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link RerankOutputBuilder}, which converts rerank inference responses into output pages.
 * <p>
 * These tests verify:
 * <ul>
 *   <li>Score block creation and insertion at the correct channel</li>
 *   <li>Null position handling (shape value 0 produces null scores)</li>
 *   <li>Multi-valued position handling (aggregates multiple scores per position)</li>
 *   <li>Batched response processing</li>
 *   <li>Score-to-shape validation</li>
 * </ul>
 */
public class RerankOutputBuilderTests extends ComputeTestCase {

    public void testBuildSmallOutput() throws Exception {
        int size = between(1, 100);
        int blockCount = between(1, 20);
        int scoreChannel = between(0, blockCount);
        assertBuildOutput(size, blockCount, scoreChannel);
    }

    public void testBuildLargeOutput() throws Exception {
        int size = between(10_000, 100_000);
        int blockCount = between(1, 20);
        int scoreChannel = between(0, blockCount);
        assertBuildOutput(size, blockCount, scoreChannel);
    }

    public void testBuildOutputWithNulls() throws Exception {
        final int size = between(10, 100);
        final int inputBlockCount = between(1, 20);
        final Page inputPage = randomInputPage(size, inputBlockCount);
        final int scoreChannel = between(0, inputBlockCount);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            // Mix null and non-null responses
            if (randomBoolean()) {
                // Null response with shape [0]
                responses.add(new BulkInferenceResponseItem(null, new int[] { 0 }, currentPos));
            } else {
                // Regular response with shape [1]
                RankedDocsResults results = createRankedDocsResults(1, currentPos);
                InferenceAction.Response response = new InferenceAction.Response(results);
                responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
            }
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));

            // If scoreChannel is at the end, the score block is appended (+1 block)
            // Otherwise, it replaces an existing block (same block count)
            int expectedBlockCount = scoreChannel == inputPage.getBlockCount() ? inputPage.getBlockCount() + 1 : inputPage.getBlockCount();
            assertThat(outputPage.getBlockCount(), equalTo(expectedBlockCount));

            // Verify score block is at the correct channel
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);
            for (int pos = 0; pos < responses.size(); pos++) {
                if (responses.get(pos).inferenceResponse() == null || responses.get(pos).shape()[0] == 0) {
                    assertTrue(scoreBlock.isNull(pos));
                } else {
                    assertFalse(scoreBlock.isNull(pos));
                }
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithBatchedResponses() throws Exception {
        final int size = 30;
        final int batchSize = 10;
        final int scoreChannel = 2;
        final Page inputPage = randomInputPage(size, 3);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();
        int currentPos = 0;

        // Create 3 batched responses
        while (currentPos < size) {
            int itemsInBatch = Math.min(batchSize, size - currentPos);

            // Create shape array: one value per item in batch
            int[] shape = new int[itemsInBatch];
            Arrays.fill(shape, 1); // Each position contributes 1 document

            RankedDocsResults results = createRankedDocsResults(itemsInBatch, currentPos);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, shape, currentPos));

            currentPos += itemsInBatch;
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount()));

            // Verify all scores are present
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);
            for (int pos = 0; pos < size; pos++) {
                assertFalse(scoreBlock.isNull(pos));
                double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(pos));
                assertFalse(Double.isNaN(score));
                assertFalse(Double.isInfinite(score));
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithScoreChannelAtEnd() throws Exception {
        final int size = between(10, 50);
        final int inputBlockCount = between(2, 5);
        final int scoreChannel = inputBlockCount; // At the end
        final Page inputPage = randomInputPage(size, inputBlockCount);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            RankedDocsResults results = createRankedDocsResults(1, currentPos);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            // Score block should be at the end
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);
            assertThat(scoreBlock.getPositionCount(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithScoreChannelInMiddle() throws Exception {
        final int size = between(10, 50);
        final int inputBlockCount = 5;
        final int scoreChannel = 2; // In the middle
        final Page inputPage = randomInputPage(size, inputBlockCount);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            RankedDocsResults results = createRankedDocsResults(1, currentPos);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount()));

            // Score block should be at channel 2
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);
            assertThat(scoreBlock.getPositionCount(), equalTo(size));

            // Verify original blocks are at their original positions (except scoreChannel which is replaced)
            for (int i = 0; i < inputBlockCount; i++) {
                if (i != scoreChannel) {
                    Block originalBlock = inputPage.getBlock(i);
                    Block outputBlock = outputPage.getBlock(i);
                    assertThat(outputBlock.getPositionCount(), equalTo(originalBlock.getPositionCount()));
                }
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputVerifyScoreValues() throws Exception {
        final int size = 10;
        final int scoreChannel = 1;
        final Page inputPage = randomInputPage(size, 2);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();
        List<Float> expectedScores = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            float score = randomFloat();
            expectedScores.add(score);

            RankedDocsResults.RankedDoc rankedDoc = new RankedDocsResults.RankedDoc(0, score, "doc_" + currentPos);
            RankedDocsResults results = new RankedDocsResults(List.of(rankedDoc));
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);

            // Verify score values match
            for (int pos = 0; pos < responses.size(); pos++) {
                assertFalse(scoreBlock.isNull(pos));
                double actual = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(pos));
                double expected = expectedScores.get(pos);
                assertThat(actual, equalTo(expected));
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithUnsortedRankedDocs() throws Exception {
        final int size = 5;
        final int batchSize = 5;
        final int scoreChannel = 0;
        final Page inputPage = randomInputPage(size, 2);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        // Create a single batched response with intentionally unsorted ranked docs
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        rankedDocs.add(new RankedDocsResults.RankedDoc(4, 0.5f, "doc_4")); // Out of order
        rankedDocs.add(new RankedDocsResults.RankedDoc(1, 0.8f, "doc_1"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(3, 0.6f, "doc_3"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(0, 0.9f, "doc_0"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(2, 0.7f, "doc_2"));

        RankedDocsResults results = new RankedDocsResults(rankedDocs);
        InferenceAction.Response response = new InferenceAction.Response(results);
        int[] shape = new int[batchSize];
        for (int i = 0; i < batchSize; i++) {
            shape[i] = 1;
        }
        responses.add(new BulkInferenceResponseItem(response, shape, 0));

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);

            // Verify scores are in the correct order (sorted by index)
            // Cast float to double to avoid precision issues when comparing
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), equalTo((double) 0.9f)); // index 0
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), equalTo((double) 0.8f)); // index 1
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(2)), equalTo((double) 0.7f)); // index 2
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(3)), equalTo((double) 0.6f)); // index 3
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(4)), equalTo((double) 0.5f)); // index 4
        }

        allBreakersEmpty();
    }

    /**
     * Tests that multi-valued positions (shape value > 1) are handled correctly
     * by aggregating multiple scores using max aggregation.
     */
    public void testBuildOutputWithMultiValuedPositions() throws Exception {
        final int scoreChannel = 0;
        final Page inputPage = randomInputPage(3, 2);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        // Create a response with shape [2, 1, 3] representing:
        // Position 0: 2 documents → should get max of first 2 scores
        // Position 1: 1 document → should get the next score
        // Position 2: 3 documents → should get max of last 3 scores
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        rankedDocs.add(new RankedDocsResults.RankedDoc(0, 0.5f, "doc_0_0"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(1, 0.8f, "doc_0_1")); // Max for position 0
        rankedDocs.add(new RankedDocsResults.RankedDoc(2, 0.6f, "doc_1"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(3, 0.7f, "doc_2_0"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(4, 0.9f, "doc_2_1")); // Max for position 2
        rankedDocs.add(new RankedDocsResults.RankedDoc(5, 0.4f, "doc_2_2"));

        RankedDocsResults results = new RankedDocsResults(rankedDocs);
        InferenceAction.Response response = new InferenceAction.Response(results);
        int[] shape = new int[] { 2, 1, 3 };
        responses.add(new BulkInferenceResponseItem(response, shape, 0));

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);

            // Verify aggregation: position 0 gets max(0.5, 0.8) = 0.8
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), equalTo((double) 0.8f));
            // Position 1 gets 0.6
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), equalTo((double) 0.6f));
            // Position 2 gets max(0.7, 0.9, 0.4) = 0.9
        }

        allBreakersEmpty();
    }

    /**
     * Tests that a mismatch between score count and shape sum throws an exception.
     */
    public void testShapeValidationThrowsOnMismatch() throws Exception {
        final int scoreChannel = 0;

        try (Page inputPage = randomInputPage(2, 1)) {
            RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);

            // Create response with 3 scores but shape [1, 1] (sum = 2)
            List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
            rankedDocs.add(new RankedDocsResults.RankedDoc(0, 0.5f, "doc_0"));
            rankedDocs.add(new RankedDocsResults.RankedDoc(1, 0.6f, "doc_1"));
            rankedDocs.add(new RankedDocsResults.RankedDoc(2, 0.7f, "doc_2")); // Extra score

            RankedDocsResults results = new RankedDocsResults(rankedDocs);
            InferenceAction.Response response = new InferenceAction.Response(results);
            int[] shape = new int[] { 1, 1 }; // Sum is 2, but we have 3 scores
            List<BulkInferenceResponseItem> responses = List.of(new BulkInferenceResponseItem(response, shape, 0));

            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> outputBuilder.buildOutputPage(inputPage, responses)
            );
            assertThat(exception.getMessage(), equalTo("Mismatch between score count and shape: expected 2 scores but got 3"));
        }

        allBreakersEmpty();
    }

    /**
     * Tests that positions with mixed shape values (including zeros) are handled correctly.
     */
    public void testBuildOutputWithMixedShapeValues() throws Exception {
        final int scoreChannel = 1;
        final Page inputPage = randomInputPage(4, 2);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        // Shape [1, 0, 2, 0] representing: doc, null, multi-valued, null
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        rankedDocs.add(new RankedDocsResults.RankedDoc(0, 0.5f, "doc_0"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(1, 0.7f, "doc_2_0"));
        rankedDocs.add(new RankedDocsResults.RankedDoc(2, 0.6f, "doc_2_1"));

        RankedDocsResults results = new RankedDocsResults(rankedDocs);
        InferenceAction.Response response = new InferenceAction.Response(results);
        int[] shape = new int[] { 1, 0, 2, 0 };
        List<BulkInferenceResponseItem> responses = List.of(new BulkInferenceResponseItem(response, shape, 0));

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            DoubleBlock scoreBlock = outputPage.getBlock(scoreChannel);

            assertFalse(scoreBlock.isNull(0));
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), equalTo((double) 0.5f));

            assertTrue(scoreBlock.isNull(1)); // Position with shape 0

            assertFalse(scoreBlock.isNull(2));
            assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(2)), equalTo((double) 0.7f)); // max(0.7, 0.6)

            assertTrue(scoreBlock.isNull(3)); // Position with shape 0
        }

        allBreakersEmpty();
    }

    private void assertBuildOutput(int size, int blockCount, int scoreChannel) throws Exception {
        final Page inputPage = randomInputPage(size, blockCount);

        RerankOutputBuilder outputBuilder = new RerankOutputBuilder(blockFactory(), scoreChannel);
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            RankedDocsResults results = createRankedDocsResults(1, currentPos);
            InferenceAction.Response response = new InferenceAction.Response(results);
            responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));

            // If scoreChannel is at the end, the score block is appended (+1 block)
            // Otherwise, it replaces an existing block (same block count)
            int expectedBlockCount = scoreChannel == inputPage.getBlockCount() ? inputPage.getBlockCount() + 1 : inputPage.getBlockCount();
            assertThat(outputPage.getBlockCount(), equalTo(expectedBlockCount));
            assertOutputContent(outputPage.getBlock(scoreChannel));
        }

        allBreakersEmpty();
    }

    private void assertOutputContent(DoubleBlock block) {
        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            double score = block.getDouble(block.getFirstValueIndex(currentPos));
            assertFalse(Double.isNaN(score));
            assertFalse(Double.isInfinite(score));
        }
    }

    private RankedDocsResults createRankedDocsResults(int count, int startIndex) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            float score = randomFloat();
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, score, "doc_" + (startIndex + i)));
        }
        return new RankedDocsResults(rankedDocs);
    }

    private Page randomInputPage(int positionCount, int columnCount) {
        final Block[] blocks = new Block[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory(),
                    RandomBlock.randomElementExcluding(List.of(ElementType.AGGREGATE_METRIC_DOUBLE)),
                    positionCount,
                    randomBoolean(),
                    0,
                    0,
                    randomInt(10),
                    randomInt(10)
                ).block();
            }

            return new Page(blocks);
        } catch (Exception e) {
            Releasables.close(blocks);
            throw (e);
        }
    }
}
