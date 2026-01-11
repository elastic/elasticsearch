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
 *   <li>Shape array correctness (tracking document count per position)</li>
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
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
            // Should produce exactly one request containing all items
            assertTrue(requestIterator.hasNext());

            BulkInferenceRequestItem requestItem = requestIterator.next();
            InferenceAction.Request request = requestItem.inferenceRequest();

            assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
            assertThat(request.getTaskType(), equalTo(TaskType.RERANK));
            assertThat(request.getQuery(), equalTo(QUERY_TEXT));
            assertThat(request.getInput().size(), equalTo(size));
            assertThat(requestItem.shape().length, equalTo(size));

            // No more requests
            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateEmptyInput() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(0);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, 20)) {
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
        final BytesRefBlock inputBlock = randomInputBlockWithNulls(size);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
            int totalPositionsProcessed = 0;
            int totalDocumentsProcessed = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                int[] shape = requestItem.shape();

                // Count positions and documents from shape
                int positionsInBatch = shape.length;
                int documentsInBatch = 0;
                for (int shapeValue : shape) {
                    documentsInBatch += shapeValue;
                }

                totalPositionsProcessed += positionsInBatch;
                totalDocumentsProcessed += documentsInBatch;

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

                    // Each shape value should be 0 or 1 for single-valued fields
                    for (int shapeValue : shape) {
                        assertTrue(shapeValue == 0 || shapeValue == 1);
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
     * creating a separate batch. The null contributes to the shape array but not to
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

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
                // First batch: 1 null position + 10 document positions
                // Shape [0,1,1,1,1,1,1,1,1,1,1] where first 0 is the leading null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem firstItem = requestIterator.next();
                assertThat(firstItem.inferenceRequest().getInput().size(), equalTo(10));
                assertThat(firstItem.shape().length, equalTo(11)); // 1 null position + 10 doc positions
                assertThat(firstItem.shape()[0], equalTo(0)); // Leading null contributes 0 documents
                for (int i = 1; i < 11; i++) {
                    assertThat(firstItem.shape()[i], equalTo(1)); // Each position contributes 1 document
                }

                // Second batch: remaining 4 document positions
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem secondItem = requestIterator.next();
                assertThat(secondItem.inferenceRequest().getInput().size(), equalTo(4));
                assertThat(secondItem.shape().length, equalTo(4));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 1000);
        final int batchSize = between(5, 50);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testBatchingBehavior() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = 25;
        final int batchSize = 10;
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
            List<Integer> batchSizes = new ArrayList<>();
            List<Integer> shapeLengths = new ArrayList<>();

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.inferenceRequest();
                batchSizes.add(request.getInput().size());
                shapeLengths.add(requestItem.shape().length);
            }

            // Should produce 3 batches: 10, 10, 5 documents
            assertThat(batchSizes.size(), equalTo(3));
            assertThat(batchSizes.get(0), equalTo(10));
            assertThat(batchSizes.get(1), equalTo(10));
            assertThat(batchSizes.get(2), equalTo(5));

            // Shape lengths should match document counts for non-null positions
            assertThat(shapeLengths.get(0), equalTo(10));
            assertThat(shapeLengths.get(1), equalTo(10));
            assertThat(shapeLengths.get(2), equalTo(5));
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

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
                // First batch: 5 document positions (hits batch size) + 3 trailing null positions
                // Shape [1,1,1,1,1,0,0,0] represents 5 docs and 3 nulls bundled together
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem firstItem = requestIterator.next();
                assertThat(firstItem.inferenceRequest().getInput().size(), equalTo(5));
                assertThat(firstItem.shape().length, equalTo(8)); // 5 doc positions + 3 null positions
                assertThat(firstItem.shape()[5], equalTo(0)); // First trailing null
                assertThat(firstItem.shape()[6], equalTo(0)); // Second trailing null
                assertThat(firstItem.shape()[7], equalTo(0)); // Third trailing null

                // Second batch: 5 document positions (hits batch size) + 2 trailing null positions
                // Shape [1,1,1,1,1,0,0] represents 5 docs and 2 nulls bundled together
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem secondItem = requestIterator.next();
                assertThat(secondItem.inferenceRequest().getInput().size(), equalTo(5));
                assertThat(secondItem.shape().length, equalTo(7)); // 5 doc positions + 2 null positions
                assertThat(secondItem.shape()[5], equalTo(0)); // First trailing null
                assertThat(secondItem.shape()[6], equalTo(0)); // Second trailing null

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests support for multi-valued fields where a single position can contribute
     * multiple documents to the inference request. The shape array should reflect
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

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Total documents: 1 + 2 + 3 + 1 = 7
                assertThat(item.inferenceRequest().getInput().size(), equalTo(7));

                // Shape should be [1, 2, 3, 1]
                assertThat(item.shape().length, equalTo(4));
                assertThat(item.shape()[0], equalTo(1));
                assertThat(item.shape()[1], equalTo(2));
                assertThat(item.shape()[2], equalTo(3));
                assertThat(item.shape()[3], equalTo(1));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests that empty and whitespace-only strings are filtered out and treated
     * as null positions (shape value of 0). This prevents sending invalid input
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

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Only 2 valid documents (empty and whitespace filtered out)
                assertThat(item.inferenceRequest().getInput().size(), equalTo(2));

                // Shape should be [1, 0, 0, 1] - empty strings treated as null
                assertThat(item.shape().length, equalTo(4));
                assertThat(item.shape()[0], equalTo(1));
                assertThat(item.shape()[1], equalTo(0));
                assertThat(item.shape()[2], equalTo(0));
                assertThat(item.shape()[3], equalTo(1));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests the edge case where all positions are null. This should produce a single
     * batch with a null request and a shape array of all zeros.
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

            try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
                // Should produce one batch with no documents but shape [0,0,0,0,0]
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                // Null request since no documents
                assertThat(item.inferenceRequest(), nullValue());

                // Shape should have 5 zeros
                assertThat(item.shape().length, equalTo(5));
                for (int i = 0; i < 5; i++) {
                    assertThat(item.shape()[i], equalTo(0));
                }

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    private void assertIterate(int size, int batchSize) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (RerankRequestIterator requestIterator = new RerankRequestIterator(inferenceId, QUERY_TEXT, inputBlock, batchSize)) {
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

                // Verify shape matches batch size
                assertThat(requestItem.shape().length, equalTo(batchItemCount));
                for (int shapeValue : requestItem.shape()) {
                    assertThat(shapeValue, equalTo(1));
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
