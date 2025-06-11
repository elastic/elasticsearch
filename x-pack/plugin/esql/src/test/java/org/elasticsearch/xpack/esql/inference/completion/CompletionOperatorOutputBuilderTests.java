/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CompletionOperatorOutputBuilderTests extends ComputeTestCase {

    public void testBuildSmallOutput() {
        assertBuildOutput(between(1, 100));
    }

    public void testBuildLargeOutput() {
        assertBuildOutput(between(10_000, 100_000));
    }

    private void assertBuildOutput(int size) {
        final Page inputPage = randomInputPage(size, between(1, 20));
        try (
            CompletionOperatorOutputBuilder outputBuilder = new CompletionOperatorOutputBuilder(
                blockFactory().newBytesRefBlockBuilder(size),
                inputPage
            )
        ) {
            for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
                List<ChatCompletionResults.Result> results = List.of(new ChatCompletionResults.Result("Completion result #" + currentPos));
                outputBuilder.addInferenceResponse(new InferenceAction.Response(new ChatCompletionResults(results)));
            }

            final Page outputPage = outputBuilder.buildOutput();
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
            assertOutputContent(outputPage.getBlock(outputPage.getBlockCount() - 1));

            outputPage.releaseBlocks();

        } finally {
            inputPage.releaseBlocks();
        }

    }

    private void assertOutputContent(BytesRefBlock block) {
        BytesRef scratch = new BytesRef();

        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            scratch = block.getBytesRef(block.getFirstValueIndex(currentPos), scratch);
            assertThat(scratch.utf8ToString(), equalTo("Completion result #" + currentPos));
        }
    }

    private Page randomInputPage(int positionCount, int columnCount) {
        final Block[] blocks = new Block[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                blocks[i] = RandomBlock.randomBlock(
                    blockFactory(),
                    RandomBlock.randomElementType(),
                    positionCount,
                    randomBoolean(),
                    0,
                    0,
                    randomInt(10),
                    randomInt(10)
                ).block();
            }

            return new Page(blocks);
        } catch (Exception e) {
            Releasables.close(blocks);
            throw (e);
        }
    }
}
