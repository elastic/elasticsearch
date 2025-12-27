/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CompletionRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterate(between(1, 100));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterate(between(10_000, 100_000));
    }

    public void testIterateEmptyInput() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(0);

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // Empty page should have no iterations
            assertFalse(requestIterator.hasNext());

            // Verify estimatedSize is also 0
            assertThat(requestIterator.estimatedSize(), equalTo(0));
        }

        allBreakersEmpty();
    }

    public void testIterateWithNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock;

        // Create a block with pattern: [prompt1, null, null, prompt2, prompt3]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            blockBuilder.appendBytesRef(new BytesRef("prompt1"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("prompt2"));
            blockBuilder.appendBytesRef(new BytesRef("prompt3"));
            inputBlock = blockBuilder.build();
        }

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // First request: batches prompt1 with trailing nulls
            // Expected shape: [1, 0, 0] - one prompt, two nulls batched together
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem1 = requestIterator.next();
            assertThat(requestItem1.inferenceRequest().getInferenceEntityId(), equalTo(inferenceId));
            assertThat(requestItem1.inferenceRequest().getTaskType(), equalTo(TaskType.COMPLETION));
            assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("prompt1"));
            assertThat(requestItem1.shape(), equalTo(new int[] { 1, 0, 0 }));

            // Second request: prompt2 with trailing prompt3 (not batched as we hit non-null)
            // Expected shape: [1] - just prompt2
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem2 = requestIterator.next();
            assertThat(requestItem2.inferenceRequest().getInferenceEntityId(), equalTo(inferenceId));
            assertThat(requestItem2.inferenceRequest().getTaskType(), equalTo(TaskType.COMPLETION));
            assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("prompt2"));
            assertThat(requestItem2.shape(), equalTo(new int[] { 1 }));

            // Third request: prompt3
            // Expected shape: [1]
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem3 = requestIterator.next();
            assertThat(requestItem3.inferenceRequest().getInferenceEntityId(), equalTo(inferenceId));
            assertThat(requestItem3.inferenceRequest().getTaskType(), equalTo(TaskType.COMPLETION));
            assertThat(requestItem3.inferenceRequest().getInput().getFirst(), equalTo("prompt3"));
            assertThat(requestItem3.shape(), equalTo(new int[] { 1 }));

            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateWithLeadingNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock;

        // Create a block with pattern: [null, null, prompt1, prompt2]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(4)) {
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("prompt1"));
            blockBuilder.appendBytesRef(new BytesRef("prompt2"));
            inputBlock = blockBuilder.build();
        }

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // First request: skips leading nulls and finds prompt1
            // Expected shape: [0, 0, 1] - two nulls, then prompt1
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem1 = requestIterator.next();
            assertThat(requestItem1.inferenceRequest().getInferenceEntityId(), equalTo(inferenceId));
            assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("prompt1"));
            assertThat(requestItem1.shape(), equalTo(new int[] { 0, 0, 1 }));

            // Second request: prompt2
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem2 = requestIterator.next();
            assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("prompt2"));
            assertThat(requestItem2.shape(), equalTo(new int[] { 1 }));

            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateWithTrailingNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock;

        // Create a block with pattern: [prompt1, prompt2, null, null]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(4)) {
            blockBuilder.appendBytesRef(new BytesRef("prompt1"));
            blockBuilder.appendBytesRef(new BytesRef("prompt2"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            inputBlock = blockBuilder.build();
        }

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // First request: prompt1
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem1 = requestIterator.next();
            assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("prompt1"));
            assertThat(requestItem1.shape(), equalTo(new int[] { 1 }));

            // Second request: prompt2 with trailing nulls batched
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem2 = requestIterator.next();
            assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("prompt2"));
            assertThat(requestItem2.shape(), equalTo(new int[] { 1, 0, 0 }));

            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateWithAllNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock;

        // Create a block with all nulls: [null, null, null]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            inputBlock = blockBuilder.build();
        }

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // Single request with all nulls
            // Expected shape: [0, 0, 0]
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem = requestIterator.next();
            assertThat(requestItem.inferenceRequest(), nullValue());
            assertThat(requestItem.shape(), equalTo(new int[] { 0, 0, 0 }));

            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testIterateWithMixedNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock;

        // Create a block with mixed pattern: [null, prompt1, null, prompt2, null, null, prompt3]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(7)) {
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("prompt1"));
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("prompt2"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("prompt3"));
            inputBlock = blockBuilder.build();
        }

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            // First request: leading null + prompt1 + trailing null
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem1 = requestIterator.next();
            assertThat(requestItem1.inferenceRequest().getInput().getFirst(), equalTo("prompt1"));
            assertThat(requestItem1.shape(), equalTo(new int[] { 0, 1, 0 }));

            // Second request: prompt2 + trailing nulls
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem2 = requestIterator.next();
            assertThat(requestItem2.inferenceRequest().getInput().getFirst(), equalTo("prompt2"));
            assertThat(requestItem2.shape(), equalTo(new int[] { 1, 0, 0 }));

            // Third request: prompt3
            assertTrue(requestIterator.hasNext());
            BulkInferenceRequestItem requestItem3 = requestIterator.next();
            assertThat(requestItem3.inferenceRequest().getInput().getFirst(), equalTo("prompt3"));
            assertThat(requestItem3.shape(), equalTo(new int[] { 1 }));

            assertFalse(requestIterator.hasNext());
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 1000);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    private void assertIterate(int size) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (CompletionRequestIterator requestIterator = new CompletionRequestIterator(inferenceId, inputBlock)) {
            BytesRef scratch = new BytesRef();
            int iterationCount = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.inferenceRequest();

                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(request.getTaskType(), equalTo(TaskType.COMPLETION));

                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(iterationCount), scratch);
                assertThat(request.getInput().getFirst(), equalTo(scratch.utf8ToString()));

                // Verify shape is [1] for simple completion requests
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
