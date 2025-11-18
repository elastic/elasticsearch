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

import static org.hamcrest.Matchers.equalTo;

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

        try (CompletionOperatorRequestIterator requestIterator = new CompletionOperatorRequestIterator(inputBlock, inferenceId)) {
            BytesRef scratch = new BytesRef();

            for (int currentPos = 0; requestIterator.hasNext(); currentPos++) {
                InferenceAction.Request request = requestIterator.next();
                assertThat(request.getInferenceEntityId(), equalTo(inferenceId));
                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(currentPos), scratch);
                assertThat(request.getInput().getFirst(), equalTo(scratch.utf8ToString()));
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
