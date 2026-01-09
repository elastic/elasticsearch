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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RerankOperatorRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterate(between(1, 100), randomIntBetween(1, 1_000));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterate(between(10_000, 100_000), randomIntBetween(1, 1_000));
    }

    private void assertIterate(int size, int batchSize) throws Exception {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (
            RerankInferenceRequestIterator requestIterator = new RerankInferenceRequestIterator(
                inputBlock,
                inferenceId,
                queryText,
                batchSize
            )
        ) {
            BytesRef scratch = new BytesRef();

            for (int currentPos = 0; requestIterator.hasNext();) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                InferenceAction.Request request = requestItem.request();

                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(request.getQuery(), equalTo(queryText));
                List<String> inputs = request.getInput();
                for (String input : inputs) {
                    scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(currentPos), scratch);
                    assertThat(input, equalTo(scratch.utf8ToString()));
                    currentPos++;
                }
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

    public void testShapeTrackingSingleValues() throws Exception {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();
        final int batchSize = 10;

        // Create block with single values per position: ["doc1", "doc2", "doc3"]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            blockBuilder.appendBytesRef(new BytesRef("doc1"));
            blockBuilder.appendBytesRef(new BytesRef("doc2"));
            blockBuilder.appendBytesRef(new BytesRef("doc3"));

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                RerankInferenceRequestIterator requestIterator = new RerankInferenceRequestIterator(
                    inputBlock,
                    inferenceId,
                    queryText,
                    batchSize
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                assertThat(item.request().getInput(), equalTo(List.of("doc1", "doc2", "doc3")));
                assertThat(item.shape(), equalTo(new int[] { 1, 1, 1 })); // Each position has 1 value

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testShapeTrackingMultiValues() throws Exception {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();
        final int batchSize = 10;

        // Create block with multi-values: [["a", "b"], ["c"], ["d", "e", "f"]]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            // Position 0: 2 values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("a"));
            blockBuilder.appendBytesRef(new BytesRef("b"));
            blockBuilder.endPositionEntry();

            // Position 1: 1 value
            blockBuilder.appendBytesRef(new BytesRef("c"));

            // Position 2: 3 values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("d"));
            blockBuilder.appendBytesRef(new BytesRef("e"));
            blockBuilder.appendBytesRef(new BytesRef("f"));
            blockBuilder.endPositionEntry();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                RerankInferenceRequestIterator requestIterator = new RerankInferenceRequestIterator(
                    inputBlock,
                    inferenceId,
                    queryText,
                    batchSize
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();

                assertThat(item.request().getInput(), equalTo(List.of("a", "b", "c", "d", "e", "f")));
                assertThat(item.shape(), equalTo(new int[] { 2, 1, 3 })); // 2, 1, and 3 values per position

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testShapeTrackingWithNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();
        final int batchSize = 10;

        // Create block: ["doc1", null, "doc2", null]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(4)) {
            blockBuilder.appendBytesRef(new BytesRef("doc1"));
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("doc2"));
            blockBuilder.appendNull();

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                RerankInferenceRequestIterator requestIterator = new RerankInferenceRequestIterator(
                    inputBlock,
                    inferenceId,
                    queryText,
                    batchSize
                )
            ) {
                // Single batch: all positions including nulls
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(item.request().getInput(), equalTo(List.of("doc1", "doc2")));
                assertThat(item.shape(), equalTo(new int[] { 1, 0, 1, 0 })); // value, null, value, null

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testShapeTrackingWithBatching() throws Exception {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();
        final int batchSize = 2; // Small batch to force multiple requests

        // Create block: [["a", "b"], ["c"], ["d", "e", "f"], ["g"]]
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(4)) {
            // Position 0: 2 values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("a"));
            blockBuilder.appendBytesRef(new BytesRef("b"));
            blockBuilder.endPositionEntry();

            // Position 1: 1 value
            blockBuilder.appendBytesRef(new BytesRef("c"));

            // Position 2: 3 values
            blockBuilder.beginPositionEntry();
            blockBuilder.appendBytesRef(new BytesRef("d"));
            blockBuilder.appendBytesRef(new BytesRef("e"));
            blockBuilder.appendBytesRef(new BytesRef("f"));
            blockBuilder.endPositionEntry();

            // Position 3: 1 value
            blockBuilder.appendBytesRef(new BytesRef("g"));

            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                RerankInferenceRequestIterator requestIterator = new RerankInferenceRequestIterator(
                    inputBlock,
                    inferenceId,
                    queryText,
                    batchSize
                )
            ) {
                // First batch: positions 0-1
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item1 = requestIterator.next();
                assertThat(item1.request().getInput(), equalTo(List.of("a", "b", "c")));
                assertThat(item1.shape(), equalTo(new int[] { 2, 1 }));

                // Second batch: positions 2-3
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem item2 = requestIterator.next();
                assertThat(item2.request().getInput(), equalTo(List.of("d", "e", "f", "g")));
                assertThat(item2.shape(), equalTo(new int[] { 3, 1 }));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }
}
