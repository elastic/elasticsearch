/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TextEmbeddingRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterate(between(1, 100));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterate(between(1_000, 10_000));
    }

    public void testIterateEmptyInput() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(0);

        try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
            // Empty page should have no iterations
            assertFalse(requestIterator.hasNext());

            // Verify estimatedSize is also 0
            assertThat(requestIterator.estimatedSize(), equalTo(0));
        }

        allBreakersEmpty();
    }

    public void testIterateWithNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 100);
        final BytesRefBlock inputBlock = randomInputBlockWithNulls(size);

        try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
            int totalPositionsProcessed = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();

                // Verify shape array covers the positions in this batch
                int[] shape = requestItem.shape();
                assertTrue("Shape must not be empty", shape.length > 0);

                // Count how many positions are in this batch
                int positionsInBatch = shape.length;
                totalPositionsProcessed += positionsInBatch;

                // Count non-null positions in the shape
                int nonNullCount = 0;
                for (int value : shape) {
                    if (value > 0) {
                        nonNullCount++;
                    }
                }

                // Verify request matches the non-null count
                if (nonNullCount == 0) {
                    // All positions in this batch are null
                    assertThat(requestItem.inferenceRequest(), nullValue());
                } else {
                    // Should have exactly one non-null position (the iterator processes one text at a time)
                    assertThat("Each batch should have exactly one non-null position", nonNullCount, equalTo(1));

                    InferenceAction.Request request = requestItem.inferenceRequest();
                    assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                    assertThat(request.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));
                    assertThat(request.getInput().size(), equalTo(1));
                }
            }

            // Verify all positions were processed
            assertThat(totalPositionsProcessed, equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testIterateWithLeadingNulls() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block with leading nulls: [null, null, "text1", "text2"]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(4)) {
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("text1"));
            blockBuilder.appendBytesRef(new BytesRef("text2"));

            BytesRefBlock inputBlock = blockBuilder.build();

            try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
                // First batch: skips leading nulls and processes first non-null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem1 = requestIterator.next();

                // Shape should be [0, 0, 1] for the two leading nulls and the first text
                assertThat(requestItem1.shape().length, equalTo(3));
                assertThat(requestItem1.shape()[0], equalTo(0)); // null
                assertThat(requestItem1.shape()[1], equalTo(0)); // null
                assertThat(requestItem1.shape()[2], equalTo(1)); // "text1"
                assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("text1"));

                // Second batch: processes second non-null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem2 = requestIterator.next();

                // Shape should be [1] for just the second text
                assertThat(requestItem2.shape().length, equalTo(1));
                assertThat(requestItem2.shape()[0], equalTo(1)); // "text2"
                assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("text2"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testIterateWithTrailingNulls() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block with trailing nulls: ["text1", null, null]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            blockBuilder.appendBytesRef(new BytesRef("text1"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
                // Single batch should bundle the text with trailing nulls
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                // Shape should be [1, 0, 0] for the text and two trailing nulls
                assertThat(requestItem.shape().length, equalTo(3));
                assertThat(requestItem.shape()[0], equalTo(1)); // "text1"
                assertThat(requestItem.shape()[1], equalTo(0)); // null
                assertThat(requestItem.shape()[2], equalTo(0)); // null
                assertThat(requestItem.inferenceRequest().getInput().getFirst(), equalTo("text1"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testIterateWithAllNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(5, 20);

        // Create a block with all nulls
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                blockBuilder.appendNull();
            }

            BytesRefBlock inputBlock = blockBuilder.build();

            try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
                // Should produce one batch with all nulls
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                // Shape should have all zeros
                assertThat(requestItem.shape().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(requestItem.shape()[i], equalTo(0));
                }

                // Request should be null since there's no actual text
                assertThat(requestItem.inferenceRequest(), nullValue());

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testIterateWithMixedNulls() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block: ["text1", null, "text2", null, null, "text3"]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(6)) {
            blockBuilder.appendBytesRef(new BytesRef("text1"));
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("text2"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("text3"));

            BytesRefBlock inputBlock = blockBuilder.build();

            try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
                // First batch: "text1" with trailing null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem1 = requestIterator.next();
                assertThat(requestItem1.shape().length, equalTo(2));
                assertThat(requestItem1.shape()[0], equalTo(1)); // "text1"
                assertThat(requestItem1.shape()[1], equalTo(0)); // null
                assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("text1"));

                // Second batch: "text2" with trailing nulls
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem2 = requestIterator.next();
                assertThat(requestItem2.shape().length, equalTo(3));
                assertThat(requestItem2.shape()[0], equalTo(1)); // "text2"
                assertThat(requestItem2.shape()[1], equalTo(0)); // null
                assertThat(requestItem2.shape()[2], equalTo(0)); // null
                assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("text2"));

                // Third batch: "text3" alone
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem3 = requestIterator.next();
                assertThat(requestItem3.shape().length, equalTo(1));
                assertThat(requestItem3.shape()[0], equalTo(1)); // "text3"
                assertThat(requestItem3.inferenceRequest().getInput().getFirst(), equalTo("text3"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 1000);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testIterateWithMultiValuedFields() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 100);

        // Create a block with multi-valued positions
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                if (randomBoolean()) {
                    // Single value
                    blockBuilder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                } else {
                    // Multi-valued position - only first value should be used
                    blockBuilder.beginPositionEntry();
                    blockBuilder.appendBytesRef(new BytesRef("first_" + i));
                    blockBuilder.appendBytesRef(new BytesRef("second_" + i));
                    blockBuilder.endPositionEntry();
                }
            }

            BytesRefBlock inputBlock = blockBuilder.build();

            try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
                BytesRef scratch = new BytesRef();
                int iterationCount = 0;

                while (requestIterator.hasNext()) {
                    BulkInferenceRequestItem requestItem = requestIterator.next();
                    InferenceAction.Request request = requestItem.inferenceRequest();

                    assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                    assertThat(request.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));

                    // Verify only the first value is used
                    scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(iterationCount), scratch);
                    assertThat(request.getInput().getFirst(), equalTo(scratch.utf8ToString()));
                    assertThat(requestItem.shape()[0], equalTo(1));

                    iterationCount++;
                }

                assertThat(iterationCount, equalTo(size));
            }
        }

        allBreakersEmpty();
    }

    private void assertIterate(int size) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (TextEmbeddingRequestIterator requestIterator = new TextEmbeddingRequestIterator(inferenceId, inputBlock)) {
            BytesRef scratch = new BytesRef();
            int iterationCount = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.inferenceRequest();

                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(request.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));

                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(iterationCount), scratch);
                assertThat(request.getInput().getFirst(), equalTo(scratch.utf8ToString()));

                // Verify shape is [1] for simple text embedding requests
                assertThat(requestItem.shape().length, equalTo(1));
                assertThat(requestItem.shape()[0], equalTo(1));

                iterationCount++;
            }

            assertThat(iterationCount, equalTo(size));
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
