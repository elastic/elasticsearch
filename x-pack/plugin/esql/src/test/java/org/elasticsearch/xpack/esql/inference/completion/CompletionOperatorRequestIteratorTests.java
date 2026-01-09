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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CompletionOperatorRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterate(between(1, 100));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterate(between(10_000, 100_000));
    }

    private void assertIterate(int size) throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (CompletionInferenceRequestIterator requestIterator = new CompletionInferenceRequestIterator(inputBlock, inferenceId)) {
            BytesRef scratch = new BytesRef();

            for (int currentPos = 0; requestIterator.hasNext(); currentPos++) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.request();
                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(currentPos), scratch);
                assertThat(request.getInput().getFirst(), equalTo(scratch.utf8ToString()));
            }
        }

        allBreakersEmpty();
    }

    public void testIterateWithNullValues() throws Exception {
        final String inferenceId = randomIdentifier();

        // Create a block with pattern: [null, "hello", null, null, "world"]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("hello"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("world"));
            blockBuilder.appendNull();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (CompletionInferenceRequestIterator requestIterator = new CompletionInferenceRequestIterator(inputBlock, inferenceId)) {
                // First next() should skip the leading null and return "hello" (no trailing nulls consumed since more values follow)
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item1 = requestIterator.next();
                assertThat(item1.request().getInput().getFirst(), equalTo("hello"));
                assertThat(item1.shape(), equalTo(new int[] { 0, 1, 0, 0 })); // 1 leading null, value

                // Second next() should skip two nulls and return "world"
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item2 = requestIterator.next();
                assertThat(item2.request().getInput().getFirst(), equalTo("world"));
                assertThat(item2.shape(), equalTo(new int[] { 1, 0 })); // 2 leading nulls, value

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

            try (CompletionInferenceRequestIterator requestIterator = new CompletionInferenceRequestIterator(inputBlock, inferenceId)) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(item.request(), nullValue());
                assertThat(item.shape(), equalTo(new int[] { 0, 0, 0 }));

                assertFalse(requestIterator.hasNext());
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
}
