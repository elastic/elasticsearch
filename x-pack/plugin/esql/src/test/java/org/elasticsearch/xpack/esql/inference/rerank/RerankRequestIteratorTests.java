/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link RerankRequestIterator}, which converts input blocks into batched rerank inference requests.
 * <p>
 * These tests verify:
 * <ul>
 *   <li>Batching behavior (grouping positions up to batchSize documents)</li>
 *   <li>Multi-valued field support (multiple documents per position)</li>
 *   <li>Null and empty position handling</li>
 *   <li>Trailing null bundling (nulls after batch size bundled with current batch)</li>
 *   <li>Position value counts correctness (tracking document count per position)</li>
 * </ul>
 */
public class RerankRequestIteratorTests extends ComputeTestCase {
    private static final String QUERY_TEXT = "test query";

    public void testIterateSmallInputWithSmallBatch() throws Exception {
        assertIterate(between(1, 50), between(5, 10));
    }

    public void testIterateLargeInputWithLargeBatch() throws Exception {
        assertIterate(between(1000, 5000), between(50, 100));
    }

    public void testIterateWithBatchSizeLargerThanInput() throws Exception {
        int size = between(10, 50);
        int batchSize = size * 2;

        final String inferenceId = randomIdentifier();
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlock(size) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
            // Should produce exactly one request containing all items
            assertTrue(requestIterator.hasNext());

            BulkInferenceRequestItem requestItem = requestIterator.next();
            InferenceAction.Request request = requestItem.inferenceRequest();

            assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
            assertThat(request.getTaskType(), equalTo(TaskType.RERANK));
            assertThat(request.getQuery(), equalTo(QUERY_TEXT));
            assertThat(request.getInput().size(), equalTo(size));
            assertThat(requestItem.positionValueCounts().length, equalTo(size));

            // No more requests
            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateEmptyInput() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlock(0) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, 20)) {
            // Empty page should have no iterations
            assertFalse(requestIterator.hasNext());

            // Verify estimatedSize is also 0
            assertThat(requestIterator.estimatedSize(), equalTo(0));
        }

        allBreakersEmpty();
    }

    public void testIterateWithNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(30, 60);
        final int batchSize = 10;
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlockWithNulls(size) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
            int totalPositionsProcessed = 0;
            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                int[] positionValueCounts = requestItem.positionValueCounts();

                // Count positions and documents from position value counts
                int positionsInBatch = positionValueCounts.length;
                int documentsInBatch = 0;
                for (int valueCount : positionValueCounts) {
                    documentsInBatch += valueCount;
                }

                totalPositionsProcessed += positionsInBatch;

                if (requestItem.inferenceRequest() == null) {
                    // All positions were null/empty, so no documents
                    assertThat(documentsInBatch, equalTo(0));
                } else {
                    // Non-null request should have valid properties
                    InferenceAction.Request request = requestItem.inferenceRequest();
                    assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                    assertThat(request.getTaskType(), equalTo(TaskType.RERANK));
                    assertThat(request.getQuery(), equalTo(QUERY_TEXT));

                    // Number of documents should match input size
                    assertThat(request.getInput().size(), equalTo(documentsInBatch));

                    // Documents in batch should not exceed batchSize
                    assertThat(documentsInBatch, lessThanOrEqualTo(batchSize));

                    // Each value count should be 0 or 1 for single-valued fields
                    for (int valueCount : positionValueCounts) {
                        assertTrue(valueCount == 0 || valueCount == 1);
                    }
                }
            }

            // Verify all positions were processed
            assertThat(totalPositionsProcessed, equalTo(size));
        }

        allBreakersEmpty();
    }

    /**
     * Tests that a leading null position is included in the first batch rather than
     * creating a separate batch. The null contributes to the position value counts array but not to
     * the document count.
     */
    public void testIterateWithLeadingNull() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create a block with a null at the start
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(15)) {
            blockBuilder.appendNull();
            for (int i = 0; i < 14; i++) {
                blockBuilder.appendBytesRef(new BytesRef("doc_" + i));
            }

            BytesRefBlock inputBlock = blockBuilder.build();
            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { inputBlock };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                // First batch: 1 null position + 10 document positions
                // Position value counts [0,1,1,1,1,1,1,1,1,1,1] where first 0 is the leading null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem firstItem = requestIterator.next();
                assertThat(firstItem.inferenceRequest().getInput().size(), equalTo(10));
                assertThat(firstItem.positionValueCounts().length, equalTo(11)); // 1 null position + 10 doc positions
                assertThat(firstItem.positionValueCounts()[0], equalTo(0)); // Leading null contributes 0 documents
                for (int i = 1; i < 11; i++) {
                    assertThat(firstItem.positionValueCounts()[i], equalTo(1)); // Each position contributes 1 document
                }

                // Second batch: remaining 4 document positions
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem secondItem = requestIterator.next();
                assertThat(secondItem.inferenceRequest().getInput().size(), equalTo(4));
                assertThat(secondItem.positionValueCounts().length, equalTo(4));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 1000);
        final int batchSize = between(5, 50);
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlock(size) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testBatchingBehavior() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = 25;
        final int batchSize = 10;
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlock(size) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
            List<Integer> batchSizes = new ArrayList<>();
            List<Integer> positionValueCountsLengths = new ArrayList<>();

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.inferenceRequest();
                batchSizes.add(request.getInput().size());
                positionValueCountsLengths.add(requestItem.positionValueCounts().length);
            }

            // Should produce 3 batches: 10, 10, 5 documents
            assertThat(batchSizes.size(), equalTo(3));
            assertThat(batchSizes.get(0), equalTo(10));
            assertThat(batchSizes.get(1), equalTo(10));
            assertThat(batchSizes.get(2), equalTo(5));

            // Position value counts lengths should match document counts for non-null positions
            assertThat(positionValueCountsLengths.get(0), equalTo(10));
            assertThat(positionValueCountsLengths.get(1), equalTo(10));
            assertThat(positionValueCountsLengths.get(2), equalTo(5));
        }

        allBreakersEmpty();
    }

    /**
     * Tests that trailing null or empty positions are bundled with the current batch
     * instead of creating a separate batch. This optimization reduces the number of
     * inference requests when nulls appear after reaching the batch size.
     */
    public void testTrailingNullsConsumedWithBatch() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 5;

        // Create a block: 5 docs, 3 nulls, 5 docs, 2 nulls
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(15)) {
            for (int i = 0; i < 5; i++) {
                blockBuilder.appendBytesRef(new BytesRef("doc_" + i));
            }
            for (int i = 0; i < 3; i++) {
                blockBuilder.appendNull();
            }
            for (int i = 5; i < 10; i++) {
                blockBuilder.appendBytesRef(new BytesRef("doc_" + i));
            }
            for (int i = 0; i < 2; i++) {
                blockBuilder.appendNull();
            }

            BytesRefBlock inputBlock = blockBuilder.build();
            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { inputBlock };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                // First batch: 5 document positions (hits batch size) + 3 trailing null positions
                // Position value counts [1,1,1,1,1,0,0,0] represents 5 docs and 3 nulls bundled together
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem firstItem = requestIterator.next();
                assertThat(firstItem.inferenceRequest().getInput().size(), equalTo(5));
                assertThat(firstItem.positionValueCounts().length, equalTo(8)); // 5 doc positions + 3 null positions
                assertThat(firstItem.positionValueCounts()[5], equalTo(0)); // First trailing null
                assertThat(firstItem.positionValueCounts()[6], equalTo(0)); // Second trailing null
                assertThat(firstItem.positionValueCounts()[7], equalTo(0)); // Third trailing null

                // Second batch: 5 document positions (hits batch size) + 2 trailing null positions
                // Position value counts [1,1,1,1,1,0,0] represents 5 docs and 2 nulls bundled together
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem secondItem = requestIterator.next();
                assertThat(secondItem.inferenceRequest().getInput().size(), equalTo(5));
                assertThat(secondItem.positionValueCounts().length, equalTo(7)); // 5 doc positions + 2 null positions
                assertThat(secondItem.positionValueCounts()[5], equalTo(0)); // First trailing null
                assertThat(secondItem.positionValueCounts()[6], equalTo(0)); // Second trailing null

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests support for multi-valued fields where a single position can contribute
     * multiple documents to the inference request. The position value counts array should reflect
     * the actual number of documents from each position.
     */
    public void testMultiValuedFields() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create a block with multi-valued positions
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(10)) {
            // Position 0: single value
            blockBuilder.appendBytesRef(new BytesRef("doc_0"));
            // Position 1: two values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("doc_1a"));
            blockBuilder.appendBytesRef(new BytesRef("doc_1b"));
            blockBuilder.endPositionEntry();
            // Position 2: three values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("doc_2a"));
            blockBuilder.appendBytesRef(new BytesRef("doc_2b"));
            blockBuilder.appendBytesRef(new BytesRef("doc_2c"));
            blockBuilder.endPositionEntry();
            // Position 3: single value
            blockBuilder.appendBytesRef(new BytesRef("doc_3"));

            BytesRefBlock inputBlock = blockBuilder.build();
            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { inputBlock };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Total documents: 1 + 2 + 3 + 1 = 7
                assertThat(item.inferenceRequest().getInput().size(), equalTo(7));

                // Position value counts should be [1, 2, 3, 1]
                assertThat(item.positionValueCounts().length, equalTo(4));
                assertThat(item.positionValueCounts()[0], equalTo(1));
                assertThat(item.positionValueCounts()[1], equalTo(2));
                assertThat(item.positionValueCounts()[2], equalTo(3));
                assertThat(item.positionValueCounts()[3], equalTo(1));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests that empty and whitespace-only strings are filtered out and treated
     * as null positions (value count of 0). This prevents sending invalid input
     * to the inference service.
     */
    public void testEmptyStringsFiltered() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create a block with empty and whitespace-only strings
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            blockBuilder.appendBytesRef(new BytesRef("doc_0"));
            blockBuilder.appendBytesRef(new BytesRef("")); // Empty string
            blockBuilder.appendBytesRef(new BytesRef("   ")); // Whitespace only
            blockBuilder.appendBytesRef(new BytesRef("doc_1"));

            BytesRefBlock inputBlock = blockBuilder.build();
            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { inputBlock };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Only 2 valid documents (empty and whitespace filtered out)
                assertThat(item.inferenceRequest().getInput().size(), equalTo(2));

                // Position value counts should be [1, 0, 0, 1] - empty strings treated as null
                assertThat(item.positionValueCounts().length, equalTo(4));
                assertThat(item.positionValueCounts()[0], equalTo(1));
                assertThat(item.positionValueCounts()[1], equalTo(0));
                assertThat(item.positionValueCounts()[2], equalTo(0));
                assertThat(item.positionValueCounts()[3], equalTo(1));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests the edge case where all positions are null. This should produce a single
     * batch with a null request and a position value counts array of all zeros.
     */
    public void testAllNullPositions() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create a block with only null positions
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            for (int i = 0; i < 5; i++) {
                blockBuilder.appendNull();
            }

            BytesRefBlock inputBlock = blockBuilder.build();
            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { inputBlock };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                // Should produce one batch with no documents but position value counts [0,0,0,0,0]
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Null request since no documents
                assertThat(item.inferenceRequest(), nullValue());

                // Position value counts should have 5 zeros
                assertThat(item.positionValueCounts().length, equalTo(5));
                for (int i = 0; i < 5; i++) {
                    assertThat(item.positionValueCounts()[i], equalTo(0));
                }

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests support for multiple input blocks where each block contributes values
     * to the same position. Values from all blocks are combined.
     */
    public void testMultipleInputBlocks() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create two input blocks with different values at each position
        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1: ["a", "b", "c"]
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            blockBuilder1.appendBytesRef(new BytesRef("b"));
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2: ["x", "y", "z"]
            blockBuilder2.appendBytesRef(new BytesRef("x"));
            blockBuilder2.appendBytesRef(new BytesRef("y"));
            blockBuilder2.appendBytesRef(new BytesRef("z"));

            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { blockBuilder1.build(), blockBuilder2.build() };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Each position contributes 2 values (one from each block)
                // Total: 3 positions * 2 values = 6 documents
                assertThat(item.inferenceRequest().getInput().size(), equalTo(6));

                // Position value counts should contain [2, 2, 2] - each position has 2 documents
                assertThat(item.positionValueCounts().length, equalTo(3));
                assertThat(item.positionValueCounts()[0], equalTo(2));
                assertThat(item.positionValueCounts()[1], equalTo(2));
                assertThat(item.positionValueCounts()[2], equalTo(2));

                // Verify the combined inputs: [a, x, b, y, c, z]
                List<String> inputs = item.inferenceRequest().getInput();
                assertThat(inputs.get(0), equalTo("a"));
                assertThat(inputs.get(1), equalTo("x"));
                assertThat(inputs.get(2), equalTo("b"));
                assertThat(inputs.get(3), equalTo("y"));
                assertThat(inputs.get(4), equalTo("c"));
                assertThat(inputs.get(5), equalTo("z"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests that when one block is null at a position but another block has a value,
     * only the non-null block's values are included in the input.
     */
    public void testMultipleInputBlocksWithPartialNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create two input blocks where one has nulls at some positions
        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1: ["a", null, "c"]
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            blockBuilder1.appendNull();
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2: [null, "y", "z"]
            blockBuilder2.appendNull();
            blockBuilder2.appendBytesRef(new BytesRef("y"));
            blockBuilder2.appendBytesRef(new BytesRef("z"));

            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { blockBuilder1.build(), blockBuilder2.build() };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Position 0: only "a" (block 2 is null)
                // Position 1: only "y" (block 1 is null)
                // Position 2: "c" and "z"
                // Total: 4 documents
                assertThat(item.inferenceRequest().getInput().size(), equalTo(4));

                // Position value counts should be [1, 1, 2]
                assertThat(item.positionValueCounts().length, equalTo(3));
                assertThat(item.positionValueCounts()[0], equalTo(1));
                assertThat(item.positionValueCounts()[1], equalTo(1));
                assertThat(item.positionValueCounts()[2], equalTo(2));

                // Verify the inputs: [a, y, c, z]
                List<String> inputs = item.inferenceRequest().getInput();
                assertThat(inputs.get(0), equalTo("a"));
                assertThat(inputs.get(1), equalTo("y"));
                assertThat(inputs.get(2), equalTo("c"));
                assertThat(inputs.get(3), equalTo("z"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests multiple input blocks where each block contains multi-valued positions.
     * Values from all blocks are combined, and the position value counts reflects the total values per position.
     */
    public void testMultipleInputBlocksWithMultiValuedFields() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 20;

        // Create two input blocks with multi-valued positions
        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1:
            // Position 0: single value "a"
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            // Position 1: two values ["b1", "b2"]
            blockBuilder1.beginPositionEntry();
            blockBuilder1.appendBytesRef(new BytesRef("b1"));
            blockBuilder1.appendBytesRef(new BytesRef("b2"));
            blockBuilder1.endPositionEntry();
            // Position 2: single value "c"
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2:
            // Position 0: two values ["x1", "x2"]
            blockBuilder2.beginPositionEntry();
            blockBuilder2.appendBytesRef(new BytesRef("x1"));
            blockBuilder2.appendBytesRef(new BytesRef("x2"));
            blockBuilder2.endPositionEntry();
            // Position 1: single value "y"
            blockBuilder2.appendBytesRef(new BytesRef("y"));
            // Position 2: three values ["z1", "z2", "z3"]
            blockBuilder2.beginPositionEntry();
            blockBuilder2.appendBytesRef(new BytesRef("z1"));
            blockBuilder2.appendBytesRef(new BytesRef("z2"));
            blockBuilder2.appendBytesRef(new BytesRef("z3"));
            blockBuilder2.endPositionEntry();

            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { blockBuilder1.build(), blockBuilder2.build() };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Position 0: 1 (from block1) + 2 (from block2) = 3 values
                // Position 1: 2 (from block1) + 1 (from block2) = 3 values
                // Position 2: 1 (from block1) + 3 (from block2) = 4 values
                // Total: 10 documents
                assertThat(item.inferenceRequest().getInput().size(), equalTo(10));

                // Position value counts should be [3, 3, 4]
                assertThat(item.positionValueCounts().length, equalTo(3));
                assertThat(item.positionValueCounts()[0], equalTo(3));
                assertThat(item.positionValueCounts()[1], equalTo(3));
                assertThat(item.positionValueCounts()[2], equalTo(4));

                // Verify the order of inputs: values from block1 first, then block2 for each position
                List<String> inputs = item.inferenceRequest().getInput();
                // Position 0: a, x1, x2
                assertThat(inputs.get(0), equalTo("a"));
                assertThat(inputs.get(1), equalTo("x1"));
                assertThat(inputs.get(2), equalTo("x2"));
                // Position 1: b1, b2, y
                assertThat(inputs.get(3), equalTo("b1"));
                assertThat(inputs.get(4), equalTo("b2"));
                assertThat(inputs.get(5), equalTo("y"));
                // Position 2: c, z1, z2, z3
                assertThat(inputs.get(6), equalTo("c"));
                assertThat(inputs.get(7), equalTo("z1"));
                assertThat(inputs.get(8), equalTo("z2"));
                assertThat(inputs.get(9), equalTo("z3"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testMultipleInputBlocksAllNullAtPosition() throws Exception {
        final String inferenceId = randomIdentifier();
        final int batchSize = 10;

        // Create two input blocks where both are null at position 1
        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1: ["a", null, "c"]
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            blockBuilder1.appendNull();
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2: ["x", null, "z"]
            blockBuilder2.appendBytesRef(new BytesRef("x"));
            blockBuilder2.appendNull();
            blockBuilder2.appendBytesRef(new BytesRef("z"));

            BytesRefBlock[] inputBlocks = new BytesRefBlock[] { blockBuilder1.build(), blockBuilder2.build() };

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Position 0: "a" and "x" (2 docs)
                // Position 1: both null (0 docs)
                // Position 2: "c" and "z" (2 docs)
                // Total: 4 documents
                assertThat(item.inferenceRequest().getInput().size(), equalTo(4));

                // Position value counts should be [2, 0, 2]
                assertThat(item.positionValueCounts().length, equalTo(3));
                assertThat(item.positionValueCounts()[0], equalTo(2));
                assertThat(item.positionValueCounts()[1], equalTo(0)); // Both blocks null at position 1
                assertThat(item.positionValueCounts()[2], equalTo(2));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    private void assertIterate(int size, int batchSize) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock[] inputBlocks = new BytesRefBlock[] { randomInputBlock(size) };

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlocks, batchSize)) {
            int totalItemsProcessed = 0;
            int requestCount = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.inferenceRequest();
                requestCount++;

                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(request.getTaskType(), equalTo(TaskType.RERANK));
                assertThat(request.getQuery(), equalTo(QUERY_TEXT));

                int batchItemCount = request.getInput().size();
                assertThat(batchItemCount, lessThanOrEqualTo(batchSize));

                // Verify position value counts matches batch size
                assertThat(requestItem.positionValueCounts().length, equalTo(batchItemCount));
                for (int valueCount : requestItem.positionValueCounts()) {
                    assertThat(valueCount, equalTo(1));
                }

                totalItemsProcessed += batchItemCount;
            }

            // Verify all positions were processed
            assertThat(totalItemsProcessed, equalTo(size));

            // Verify expected number of requests
            int expectedRequestCount = (size + batchSize - 1) / batchSize;
            assertThat(requestCount, equalTo(expectedRequestCount));
        }

        allBreakersEmpty();
    }

    private BytesRefBlock randomInputBlock(int size) {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                blockBuilder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
            }
            return blockBuilder.build();
        }
    }

    private BytesRefBlock randomInputBlockWithNulls(int size) {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                if (randomBoolean()) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                }
            }
            return blockBuilder.build();
        }
    }
}
