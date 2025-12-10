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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TextEmbeddingOperatorRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterate(between(1, 100));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterate(between(10_000, 100_000));
    }

    public void testIterateWithNullValues() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block with pattern: [null, "before", null, null, "after", null]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(6)) {
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("before"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("after"));
            blockBuilder.appendNull();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)
            ) {
                // First item: "before" with surrounding nulls
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item1 = requestIterator.next();
                assertThat(item1.request().getInput().getFirst(), equalTo("before"));
                assertThat(item1.shape(), equalTo(new int[] { 0, 1, 0, 0 })); // 1 leading null, value, 2 trailing nulls

                // Second item: "after" with trailing null
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item2 = requestIterator.next();
                assertThat(item2.request().getInput().getFirst(), equalTo("after"));
                assertThat(item2.shape(), equalTo(new int[] { 1, 0 })); // value, 1 trailing null

                // No more items
                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testIterateAllNulls() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block with all nulls
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendNull();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(item.request(), nullValue());
                assertThat(item.shape(), equalTo(new int[] { 0, 0, 0 }));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testIterateWithMultiValuePositions() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = createMultiValueBlock();

        try (TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)) {
            // First position: multi-value, keep only the first value
            BulkInferenceRequestItem requestItem1 = requestIterator.next();
            InferenceAction.Request request1 = requestItem1.request();
            assertThat(request1.getInferenceEntityId(), equalTo(inferenceId));
            assertThat(request1.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));
            assertThat(request1.getInput().get(0), equalTo("first"));

            // Second position: single value
            BulkInferenceRequestItem requestItem2 = requestIterator.next();
            InferenceAction.Request request2 = requestItem2.request();
            assertThat(request2.getInferenceEntityId(), equalTo(inferenceId));
            assertThat(request2.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));
            assertThat(request2.getInput().get(0), equalTo("single"));
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = randomIntBetween(10, 1000);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testHasNextAndIteration() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = randomIntBetween(5, 50);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)) {
            int count = 0;
            while (requestIterator.hasNext()) {
                requestIterator.next();
                count++;
            }
            assertThat(count, equalTo(size));

            // Verify hasNext returns false after iteration is complete
            assertThat(requestIterator.hasNext(), equalTo(false));
        }

        allBreakersEmpty();
    }

    private void assertIterate(int size) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (TextEmbeddingInferenceRequestIterator requestIterator = new TextEmbeddingInferenceRequestIterator(inputBlock, inferenceId)) {
            BytesRef scratch = new BytesRef();

            for (int currentPos = 0; requestIterator.hasNext(); currentPos++) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.request();

                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(request.getTaskType(), equalTo(TaskType.TEXT_EMBEDDING));

                // Verify the input text matches what's in the block
                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(currentPos), scratch);
                assertThat(request.getInput().get(0), equalTo(scratch.utf8ToString()));
            }
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

    private BytesRefBlock createMultiValueBlock() {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(2)) {
            // First position: multiple values
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("first"));
            builder.appendBytesRef(new BytesRef("second"));
            builder.appendBytesRef(new BytesRef("third"));
            builder.endPositionEntry();

            // Second position: single value
            builder.appendBytesRef(new BytesRef("single"));

            return builder.build();
        }
    }
}
