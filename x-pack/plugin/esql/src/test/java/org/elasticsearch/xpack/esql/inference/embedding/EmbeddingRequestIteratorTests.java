/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;

import java.util.Base64;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class EmbeddingRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() throws Exception {
        assertIterateRequests(between(1, 100));
    }

    public void testIterateLargeInput() throws Exception {
        assertIterateRequests(between(1_000, 10_000));
    }

    public void testIterateEmptyInput() throws Exception {
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(0);

        try (
            EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                inferenceId,
                inputBlock,
                DataType.TEXT,
                InferenceAction.Request.DEFAULT_TIMEOUT
            )
        ) {
            assertFalse(requestIterator.hasNext());
            assertThat(requestIterator.estimatedSize(), equalTo(0));
        }

        allBreakersEmpty();
    }

    public void testIterateWithNulls() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 100);
        final BytesRefBlock inputBlock = randomInputBlockWithNulls(size);

        try (
            EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                inferenceId,
                inputBlock,
                DataType.TEXT,
                InferenceAction.Request.DEFAULT_TIMEOUT
            )
        ) {
            int totalPositionsProcessed = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();

                int[] positionValueCounts = requestItem.positionValueCounts();
                assertTrue("Position value counts must not be empty", positionValueCounts.length > 0);

                int positionsInBatch = positionValueCounts.length;
                totalPositionsProcessed += positionsInBatch;

                int nonNullCount = 0;
                for (int value : positionValueCounts) {
                    if (value > 0) {
                        nonNullCount++;
                    }
                }

                if (nonNullCount == 0) {
                    assertThat(requestItem.inferenceRequest(), nullValue());
                } else {
                    assertThat("Each batch should have exactly one non-null position", nonNullCount, equalTo(1));

                    assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                    EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();
                    assertThat(embeddingRequest.getInferenceEntityId(), equalTo(inferenceId));
                    assertThat(embeddingRequest.getTaskType(), equalTo(TaskType.EMBEDDING));
                    assertThat(embeddingRequest.getEmbeddingRequest().inputs().size(), equalTo(1));
                }
            }

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

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem1 = requestIterator.next();

                // Position value counts should be [0, 0, 1] for the two leading nulls and the first text
                assertThat(requestItem1.positionValueCounts().length, equalTo(3));
                assertThat(requestItem1.positionValueCounts()[0], equalTo(0)); // null
                assertThat(requestItem1.positionValueCounts()[1], equalTo(0)); // null
                assertThat(requestItem1.positionValueCounts()[2], equalTo(1)); // "text1"
                EmbeddingAction.Request request1 = (EmbeddingAction.Request) requestItem1.inferenceRequest();
                assertThat(request1.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text1"));

                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem2 = requestIterator.next();

                // Position value counts should be [1] for just the second text
                assertThat(requestItem2.positionValueCounts().length, equalTo(1));
                assertThat(requestItem2.positionValueCounts()[0], equalTo(1)); // "text2"
                EmbeddingAction.Request request2 = (EmbeddingAction.Request) requestItem2.inferenceRequest();
                assertThat(request2.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text2"));

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

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                // Position value counts should be [1, 0, 0] for the text and two trailing nulls
                assertThat(requestItem.positionValueCounts().length, equalTo(3));
                assertThat(requestItem.positionValueCounts()[0], equalTo(1)); // "text1"
                assertThat(requestItem.positionValueCounts()[1], equalTo(0)); // null
                assertThat(requestItem.positionValueCounts()[2], equalTo(0)); // null
                EmbeddingAction.Request request = (EmbeddingAction.Request) requestItem.inferenceRequest();
                assertThat(request.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text1"));

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

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                // Position value counts should have all zeros
                assertThat(requestItem.positionValueCounts().length, equalTo(size));
                for (int i = 0; i < size; i++) {
                    assertThat(requestItem.positionValueCounts()[i], equalTo(0));
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

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                // First batch: "text1" with trailing null
                BulkInferenceRequestItem requestItem1 = requestIterator.next();
                assertThat(requestItem1.positionValueCounts().length, equalTo(2));
                assertThat(requestItem1.positionValueCounts()[0], equalTo(1)); // "text1"
                assertThat(requestItem1.positionValueCounts()[1], equalTo(0)); // null
                EmbeddingAction.Request request1 = (EmbeddingAction.Request) requestItem1.inferenceRequest();
                assertThat(request1.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text1"));

                // Second batch: "text2" with trailing nulls
                BulkInferenceRequestItem requestItem2 = requestIterator.next();
                assertThat(requestItem2.positionValueCounts().length, equalTo(3));
                assertThat(requestItem2.positionValueCounts()[0], equalTo(1)); // "text2"
                assertThat(requestItem2.positionValueCounts()[1], equalTo(0)); // null
                assertThat(requestItem2.positionValueCounts()[2], equalTo(0)); // null
                EmbeddingAction.Request request2 = (EmbeddingAction.Request) requestItem2.inferenceRequest();
                assertThat(request2.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text2"));

                // Third batch: "text3" alone
                BulkInferenceRequestItem requestItem3 = requestIterator.next();
                assertThat(requestItem3.positionValueCounts().length, equalTo(1));
                assertThat(requestItem3.positionValueCounts()[0], equalTo(1)); // "text3"
                EmbeddingAction.Request request3 = (EmbeddingAction.Request) requestItem3.inferenceRequest();
                assertThat(request3.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo("text3"));

                assertFalse(requestIterator.hasNext());
            }
        }

        allBreakersEmpty();
    }

    public void testEstimatedSize() throws Exception {
        final String inferenceId = randomIdentifier();
        final int size = between(10, 1000);
        final BytesRefBlock inputBlock = randomInputBlock(size);

        try (
            EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                inferenceId,
                inputBlock,
                DataType.TEXT,
                InferenceAction.Request.DEFAULT_TIMEOUT
            )
        ) {
            assertThat(requestIterator.estimatedSize(), equalTo(size));
        }

        allBreakersEmpty();
    }

    public void testIterateWithImageType() throws Exception {
        final String inferenceId = randomIdentifier();
        final String dataUri = "data:image/jpeg;base64,VGhpcyBpcyBhbiBpbWFnZQ==";

        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(1)) {
            blockBuilder.appendBytesRef(new BytesRef(dataUri));
            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.IMAGE,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();
                assertThat(embeddingRequest.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(embeddingRequest.getTaskType(), equalTo(TaskType.EMBEDDING));

                var inferenceString = embeddingRequest.getEmbeddingRequest().inputs().getFirst().value();
                assertThat(inferenceString.dataType(), equalTo(DataType.IMAGE));
                assertThat(inferenceString.value(), equalTo(dataUri));
            }
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

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                BytesRef scratch = new BytesRef();
                int iterationCount = 0;

                while (requestIterator.hasNext()) {
                    BulkInferenceRequestItem requestItem = requestIterator.next();
                    assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                    EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();
                    assertThat(embeddingRequest.getInferenceEntityId(), equalTo(inferenceId));
                    assertThat(embeddingRequest.getTaskType(), equalTo(TaskType.EMBEDDING));

                    // Only the first value of a multi-valued field is used
                    scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(iterationCount), scratch);
                    assertThat(embeddingRequest.getEmbeddingRequest().inputs().getFirst().value().value(), equalTo(scratch.utf8ToString()));
                    assertThat(requestItem.positionValueCounts()[0], equalTo(1));

                    iterationCount++;
                }

                assertThat(iterationCount, equalTo(size));
            }
        }

        allBreakersEmpty();
    }

    public void testTypedInputCreatesEmbeddingActionRequest() throws Exception {
        final String inferenceId = randomIdentifier();

        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(1)) {
            blockBuilder.appendBytesRef(new BytesRef("data:image/jpeg;base64,VGhpcyBpcyBhbiBpbWFnZQ=="));
            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.IMAGE,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();
                assertThat(embeddingRequest.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(embeddingRequest.getTaskType(), equalTo(TaskType.EMBEDDING));

                var inputs = embeddingRequest.getEmbeddingRequest().inputs();
                assertThat(inputs.size(), equalTo(1));
                var inferenceString = inputs.getFirst().value();
                assertThat(inferenceString.dataType(), equalTo(DataType.IMAGE));
                assertThat(inferenceString.value(), equalTo("data:image/jpeg;base64,VGhpcyBpcyBhbiBpbWFnZQ=="));
            }
        }

        allBreakersEmpty();
    }

    public void testTypedInputWithTypeOnlyCreatesEmbeddingActionRequest() throws Exception {
        final String inferenceId = randomIdentifier();

        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(1)) {
            blockBuilder.appendBytesRef(new BytesRef("some text"));
            BytesRefBlock inputBlock = blockBuilder.build();

            try (
                EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                    inferenceId,
                    inputBlock,
                    DataType.TEXT,
                    InferenceAction.Request.DEFAULT_TIMEOUT
                )
            ) {
                assertTrue(requestIterator.hasNext());
                BulkInferenceRequestItem requestItem = requestIterator.next();

                assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();
                var inferenceString = embeddingRequest.getEmbeddingRequest().inputs().getFirst().value();
                assertThat(inferenceString.dataType(), equalTo(DataType.TEXT));
                assertThat(inferenceString.value(), equalTo("some text"));
            }
        }

        allBreakersEmpty();
    }

    private void assertIterateRequests(int size) throws Exception {
        final DataType dataType = randomDataType();
        final String inferenceId = randomIdentifier();
        final BytesRefBlock inputBlock = randomInputBlock(size, dataType);

        try (
            EmbeddingRequestIterator requestIterator = new EmbeddingRequestIterator(
                inferenceId,
                inputBlock,
                dataType,
                InferenceAction.Request.DEFAULT_TIMEOUT
            )
        ) {
            BytesRef scratch = new BytesRef();
            int iterationCount = 0;

            while (requestIterator.hasNext()) {
                BulkInferenceRequestItem requestItem = requestIterator.next();
                assertThat(requestItem.inferenceRequest(), instanceOf(EmbeddingAction.Request.class));
                EmbeddingAction.Request embeddingRequest = (EmbeddingAction.Request) requestItem.inferenceRequest();

                assertThat(embeddingRequest.getInferenceEntityId(), equalTo(inferenceId));
                assertThat(embeddingRequest.getTaskType(), equalTo(TaskType.EMBEDDING));

                var inferenceString = embeddingRequest.getEmbeddingRequest().inputs().getFirst().value();
                assertThat(inferenceString.dataType(), equalTo(dataType));

                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(iterationCount), scratch);
                assertThat(inferenceString.value(), equalTo(scratch.utf8ToString()));

                assertThat(requestItem.positionValueCounts().length, equalTo(1));
                assertThat(requestItem.positionValueCounts()[0], equalTo(1));

                iterationCount++;
            }

            assertThat(iterationCount, equalTo(size));
        }

        allBreakersEmpty();
    }

    private static DataType randomDataType() {
        return randomFrom(DataType.TEXT, DataType.IMAGE);
    }

    private static String randomInputValue(DataType dataType) {
        if (dataType == DataType.IMAGE) {
            return "data:image/jpeg;base64," + Base64.getEncoder().encodeToString(randomByteArrayOfLength(between(10, 100)));
        }
        return randomAlphaOfLength(10);
    }

    private BytesRefBlock randomInputBlock(int size) {
        return randomInputBlock(size, DataType.TEXT);
    }

    private BytesRefBlock randomInputBlock(int size, DataType dataType) {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                blockBuilder.appendBytesRef(new BytesRef(randomInputValue(dataType)));
            }
            return blockBuilder.build();
        }
    }

    private BytesRefBlock randomInputBlockWithNulls(int size) {
        return randomInputBlockWithNulls(size, DataType.TEXT);
    }

    private BytesRefBlock randomInputBlockWithNulls(int size, DataType dataType) {
        try (BytesRefBlock.Builder blockBuilder = blockFactory().newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                if (randomBoolean()) {
                    blockBuilder.appendNull();
                } else {
                    blockBuilder.appendBytesRef(new BytesRef(randomInputValue(dataType)));
                }
            }
            return blockBuilder.build();
        }
    }
}
