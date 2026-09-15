/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Shared batching tests for the two {@link AbstractEmbeddingRequestIterator} subclasses. The batching logic lives entirely in the
 * base class, so these tests are written once against the two request-shape hooks below.
 */
public abstract class AbstractEmbeddingRequestIteratorTestCase extends ComputeTestCase {

    /** Builds the iterator under test over {@code textBlock} with the given batch size (using TEXT input and a default timeout). */
    protected abstract AbstractEmbeddingRequestIterator newRequestIterator(String inferenceId, BytesRefBlock textBlock, int batchSize);

    /** Number of inputs carried by a request item ({@code 0} for a null request). */
    protected abstract int inputSize(BulkInferenceRequestItem item);

    /** The ordered input strings carried by a (non-null) request item. */
    protected abstract List<String> inputValues(BulkInferenceRequestItem item);

    /** Boundary row counts vs batch size (batchSize-1, batchSize, +1, exact multiple): asserts each request's input-list size. */
    public void testBatchBoundaries() throws Exception {
        int batchSize = 4;
        assertRequestInputSizes(batchSize, batchSize - 1, List.of(batchSize - 1));
        assertRequestInputSizes(batchSize, batchSize, List.of(batchSize));
        assertRequestInputSizes(batchSize, batchSize + 1, List.of(batchSize, 1));
        assertRequestInputSizes(batchSize, 2 * batchSize, List.of(batchSize, batchSize));
    }

    /** Interior nulls are recorded as zero counts but don't consume a batch slot. */
    public void testNullsInterspersedWithinBatch() throws Exception {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            blockBuilder.appendBytesRef(new BytesRef("t0"));
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("t1"));
            blockBuilder.appendBytesRef(new BytesRef("t2"));
            blockBuilder.appendBytesRef(new BytesRef("t3"));

            try (AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), blockBuilder.build(), 3)) {
                BulkInferenceRequestItem item1 = requestIterator.next();
                assertThat(inputSize(item1), equalTo(3));
                assertThat(item1.positionValueCounts(), equalTo(new int[] { 1, 0, 1, 1 }));

                BulkInferenceRequestItem item2 = requestIterator.next();
                assertThat(inputSize(item2), equalTo(1));
                assertThat(item2.positionValueCounts(), equalTo(new int[] { 1 }));

                assertFalse(requestIterator.hasNext());
            }
        }
        allBreakersEmpty();
    }

    /** Trailing nulls after a full batch are folded into that batch instead of forming a new (empty) request. */
    public void testTrailingNullsFoldedIntoBatch() throws Exception {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(6)) {
            blockBuilder.appendBytesRef(new BytesRef("t0"));
            blockBuilder.appendBytesRef(new BytesRef("t1"));
            blockBuilder.appendBytesRef(new BytesRef("t2"));
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("t3"));

            try (AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), blockBuilder.build(), 3)) {
                BulkInferenceRequestItem item1 = requestIterator.next();
                assertThat(inputSize(item1), equalTo(3));
                assertThat(item1.positionValueCounts(), equalTo(new int[] { 1, 1, 1, 0, 0 }));

                BulkInferenceRequestItem item2 = requestIterator.next();
                assertThat(inputSize(item2), equalTo(1));
                assertThat(item2.positionValueCounts(), equalTo(new int[] { 1 }));

                assertFalse(requestIterator.hasNext());
            }
        }
        allBreakersEmpty();
    }

    /** An all-null page produces a single request with a null inference request and all-zero position value counts. */
    public void testAllNullPageProducesSingleNullRequest() throws Exception {
        final int size = between(2, 15);
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                blockBuilder.appendNull();
            }

            try (
                AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(
                    randomIdentifier(),
                    blockBuilder.build(),
                    between(2, 5)
                )
            ) {
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(item.inferenceRequest(), nullValue());
                assertThat(item.positionValueCounts().length, equalTo(size));
                for (int c : item.positionValueCounts()) {
                    assertThat(c, equalTo(0));
                }
                assertFalse(requestIterator.hasNext());
            }
        }
        allBreakersEmpty();
    }

    /** batchSize == 1 reduces to the pre-batching one-request-per-non-null-row behavior. */
    public void testBatchSizeOneParityOneRequestPerNonNullRow() throws Exception {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(5)) {
            blockBuilder.appendBytesRef(new BytesRef("t0"));
            blockBuilder.appendNull();
            blockBuilder.appendBytesRef(new BytesRef("t1"));
            blockBuilder.appendBytesRef(new BytesRef("t2"));
            blockBuilder.appendNull();

            try (AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), blockBuilder.build(), 1)) {
                int requestCount = 0;
                while (requestIterator.hasNext()) {
                    assertThat(inputSize(requestIterator.next()), equalTo(1));
                    requestCount++;
                }
                assertThat(requestCount, equalTo(3));
            }
        }
        allBreakersEmpty();
    }

    /** Randomized invariants (with random nulls): no request exceeds batchSize, inputs sum to the non-null count, and every
     * position is counted exactly once. */
    public void testBatchedInvariantsWithRandomNulls() throws Exception {
        final int batchSize = between(2, 16);
        final int size = between(1, 200);
        final BytesRefBlock inputBlock = randomTextBlock(size, true);

        int nonNullPositions = 0;
        for (int p = 0; p < size; p++) {
            if (inputBlock.isNull(p) == false) {
                nonNullPositions++;
            }
        }

        try (AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), inputBlock, batchSize)) {
            int totalInputs = 0;
            int totalPositions = 0;
            int requestCount = 0;
            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(inputSize(item), lessThanOrEqualTo(batchSize));
                totalInputs += inputSize(item);
                totalPositions += item.positionValueCounts().length;
                requestCount++;
            }
            assertThat(totalInputs, equalTo(nonNullPositions));
            assertThat(totalPositions, equalTo(size));
            // A request is emitted per non-null batch, plus at most one extra for a trailing all-null remainder.
            assertThat(requestCount, lessThanOrEqualTo((nonNullPositions + batchSize - 1) / batchSize + 1));
        }
        allBreakersEmpty();
    }

    /**
     * Unlike RERANK (which drops empty/whitespace-only inputs), the embedding path sends them as real inputs (count 1): {@code ""}
     * embeds to a real vector rather than being nulled. Pins that divergence against a future refactor.
     */
    public void testEmptyAndWhitespaceStringsAreEmbeddedNotFiltered() throws Exception {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(3)) {
            blockBuilder.appendBytesRef(new BytesRef(""));
            blockBuilder.appendBytesRef(new BytesRef("   "));
            blockBuilder.appendBytesRef(new BytesRef("text"));

            try (AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), blockBuilder.build(), 10)) {
                BulkInferenceRequestItem item = requestIterator.next();
                assertThat(item.positionValueCounts(), equalTo(new int[] { 1, 1, 1 }));
                assertThat(inputValues(item), equalTo(List.of("", "   ", "text")));
                assertFalse(requestIterator.hasNext());
            }
        }
        allBreakersEmpty();
    }

    private void assertRequestInputSizes(int batchSize, int rows, List<Integer> expectedInputSizes) throws Exception {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                blockBuilder.appendBytesRef(new BytesRef("t" + i));
            }

            try (
                AbstractEmbeddingRequestIterator requestIterator = newRequestIterator(randomIdentifier(), blockBuilder.build(), batchSize)
            ) {
                List<Integer> actualInputSizes = new ArrayList<>();
                int totalPositions = 0;
                while (requestIterator.hasNext()) {
                    BulkInferenceRequestItem item = requestIterator.next();
                    actualInputSizes.add(inputSize(item));
                    for (int c : item.positionValueCounts()) {
                        assertThat(c, equalTo(1));
                    }
                    totalPositions += item.positionValueCounts().length;
                }
                assertThat(actualInputSizes, equalTo(expectedInputSizes));
                assertThat(totalPositions, equalTo(rows));
            }
        }
        allBreakersEmpty();
    }

    private BytesRefBlock randomTextBlock(int size, boolean withNulls) {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                if (withNulls && randomBoolean()) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                }
            }
            return blockBuilder.build();
        }
    }
}
