/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RerankOperatorTests extends InferenceOperatorTestCase<RankedDocsResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_rerank";
    private static final String QUERY_TEXT = "test query";
    private static final int BATCH_SIZE = 20;

    private int inputChannel;
    private int scoreChannel;

    @Before
    public void initRerankChannels() {
        inputChannel = between(0, inputsCount - 1);
        // Score channel can be anywhere in the page
        scoreChannel = between(0, inputsCount);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RerankOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            QUERY_TEXT,
            evaluatorFactory(inputChannel),
            scoreChannel,
            BATCH_SIZE
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));

        for (int curPage = 0; curPage < input.size(); curPage++) {
            Page inputPage = input.get(curPage);
            Page resultPage = results.get(curPage);

            assertEquals(inputPage.getPositionCount(), resultPage.getPositionCount());

            // If scoreChannel is at the end, the score block is appended (+1 block)
            // Otherwise, it replaces an existing block (same block count)
            int expectedBlockCount = scoreChannel == inputPage.getBlockCount() ? inputPage.getBlockCount() + 1 : inputPage.getBlockCount();
            assertEquals(expectedBlockCount, resultPage.getBlockCount());

            // Verify all original blocks are preserved (except the one replaced by the score block)
            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                if (channel != scoreChannel) {
                    Block inputBlock = inputPage.getBlock(channel);
                    Block resultBlock = resultPage.getBlock(channel);
                    assertBlockContentEquals(inputBlock, resultBlock);
                }
            }

            // Verify rerank scores in the score channel
            assertRerankResults(inputPage, resultPage);
        }
    }

    private void assertRerankResults(Page inputPage, Page resultPage) {
        BytesRefBlock inputBlock = inputPage.getBlock(inputChannel);
        DoubleBlock scoreBlock = resultPage.getBlock(scoreChannel);

        BlockStringReader blockReader = new BlockStringReader();

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            if (inputBlock.isNull(curPos)) {
                // Null inputs should produce null scores
                assertThat(scoreBlock.isNull(curPos), equalTo(true));
            } else {
                // Read all non-empty text values from this position
                List<String> inputTexts = readNonEmptyInputs(inputBlock, curPos);

                if (inputTexts.isEmpty()) {
                    // Empty/whitespace-only inputs should produce null scores (filtered by RerankRequestIterator)
                    assertThat(scoreBlock.isNull(curPos), equalTo(true));
                } else {
                    // Verify score is present
                    assertFalse(scoreBlock.isNull(curPos));
                    double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(curPos));

                    // For multi-valued positions, the score should be the max of all input scores
                    // (see RerankOutputBuilder.appendResponseToBlock)
                    double expectedScore = inputTexts.stream()
                        .mapToDouble(text -> (double) text.hashCode() / Integer.MAX_VALUE)
                        .max()
                        .orElseThrow();

                    assertThat(score, closeTo(expectedScore, 0.0001));
                }
            }
        }
    }

    /**
     * Reads all non-empty text values from a position, matching the filtering done by RerankRequestIterator.
     */
    private List<String> readNonEmptyInputs(BytesRefBlock block, int pos) {
        List<String> inputs = new ArrayList<>();
        int valueCount = block.getValueCount(pos);
        int firstValueIndex = block.getFirstValueIndex(pos);
        BytesRef scratch = new BytesRef();

        for (int i = 0; i < valueCount; i++) {
            scratch = block.getBytesRef(firstValueIndex + i, scratch);
            String text = scratch.utf8ToString();
            // Match the filtering logic in RerankRequestIterator.readInputText
            if (org.elasticsearch.common.Strings.hasText(text)) {
                inputs.add(text);
            }
        }

        return inputs;
    }

    @Override
    protected RankedDocsResults mockInferenceResult(InferenceAction.Request request) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        List<String> inputs = request.getInput();

        for (int i = 0; i < inputs.size(); i++) {
            String input = inputs.get(i);
            // Generate a deterministic relevance score based on input text
            float score = (float) input.hashCode() / Integer.MAX_VALUE;
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, score, input));
        }

        return new RankedDocsResults(rankedDocs);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("RerankOperator[]");
    }
}
