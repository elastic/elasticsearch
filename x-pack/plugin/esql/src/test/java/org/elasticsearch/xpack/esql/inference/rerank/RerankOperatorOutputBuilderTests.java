/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RerankOperatorOutputBuilderTests extends ComputeTestCase {

    public void testBuildSmallOutput() {
        assertBuildOutput(between(1, 100));
    }

    public void testBuildLargeOutput() {
        assertBuildOutput(between(10_000, 100_000));
    }

    private void assertBuildOutput(int size) {
        final Page inputPage = randomInputPage(size, between(1, 20));
        final int scoreChannel = randomIntBetween(0, inputPage.getBlockCount());
        try (
            RerankOperatorOutputBuilder outputBuilder = new RerankOperatorOutputBuilder(
                blockFactory().newDoubleBlockBuilder(size),
                inputPage,
                scoreChannel
            )
        ) {
            int batchSize = randomIntBetween(1, size);
            for (int currentPos = 0; currentPos < inputPage.getPositionCount();) {
                List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
                for (int rankedDocIndex = 0; rankedDocIndex < batchSize && currentPos < inputPage.getPositionCount(); rankedDocIndex++) {
                    rankedDocs.add(new RankedDocsResults.RankedDoc(rankedDocIndex, relevanceScore(currentPos), randomIdentifier()));
                    currentPos++;
                }

                outputBuilder.addInferenceResponse(new InferenceAction.Response(new RankedDocsResults(rankedDocs)));
            }

            final Page outputPage = outputBuilder.buildOutput();
            try {
                assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
                LogManager.getLogger(RerankOperatorOutputBuilderTests.class)
                    .info(
                        "{} , {}, {}, {}",
                        scoreChannel,
                        inputPage.getBlockCount(),
                        outputPage.getBlockCount(),
                        Math.max(scoreChannel + 1, inputPage.getBlockCount())
                    );
                assertThat(outputPage.getBlockCount(), equalTo(Integer.max(scoreChannel + 1, inputPage.getBlockCount())));
                assertOutputContent(outputPage.getBlock(scoreChannel));
            } finally {
                outputPage.releaseBlocks();
            }

        } finally {
            inputPage.releaseBlocks();
        }
    }

    private float relevanceScore(int position) {
        return (float) 1 / (1 + position);
    }

    private void assertOutputContent(DoubleBlock block) {
        for (int currentPos = 0; currentPos < block.getPositionCount(); currentPos++) {
            assertThat(block.isNull(currentPos), equalTo(false));
            assertThat(block.getValueCount(currentPos), equalTo(1));
            assertThat(block.getDouble(block.getFirstValueIndex(currentPos)), equalTo((double) relevanceScore(currentPos)));
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
