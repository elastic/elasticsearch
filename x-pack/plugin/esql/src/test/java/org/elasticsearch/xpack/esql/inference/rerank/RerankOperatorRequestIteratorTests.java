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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RerankOperatorRequestIteratorTests extends ComputeTestCase {

    public void testIterateSmallInput() {
        assertIterate(between(1, 100), randomIntBetween(1, 1_000));
    }

    public void testIterateLargeInput() {
        assertIterate(between(10_000, 100_000), randomIntBetween(1, 1_000));
    }

    private void assertIterate(int size, int batchSize) {
        final String inferenceId = randomIdentifier();
        final String queryText = randomIdentifier();

        try (
            BytesRefBlock inputBlock = randomInputBlock(size);
            RerankOperatorRequestIterator requestIterator = new RerankOperatorRequestIterator(inputBlock, inferenceId, queryText, batchSize)
        ) {
            BytesRef scratch = new BytesRef();

            for (int currentPos = 0; requestIterator.hasNext();) {
                InferenceAction.Request request = requestIterator.next();

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
