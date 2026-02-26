/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RerankOperatorTests extends InferenceOperatorTestCase<RankedDocsResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_rerank";
    private static final String QUERY_TEXT = "test query";
    private static final int BATCH_SIZE = 20;

    private int inputChannel;
    private List<Integer> inputChannels;
    private int scoreChannel;

    @Before
    public void initRerankChannels() {
        inputChannel = between(0, inputsCount - 1);
        inputChannels = List.of(inputChannel);
        // Score channel can be anywhere in the page
        scoreChannel = between(0, inputsCount);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RerankOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            QUERY_TEXT,
            inputChannels.stream().map(this::evaluatorFactory).toList(),
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
        DoubleBlock scoreBlock = resultPage.getBlock(scoreChannel);

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            // Collect all non-empty text values from all input channels at this position
            List<String> inputTexts = new ArrayList<>();
            boolean allNull = true;

            for (int channel : inputChannels) {
                BytesRefBlock inputBlock = inputPage.getBlock(channel);
                if (inputBlock.isNull(curPos) == false) {
                    allNull = false;
                    inputTexts.addAll(readNonEmptyInputs(inputBlock, curPos));
                }
            }

            if (allNull || inputTexts.isEmpty()) {
                // All null or empty/whitespace-only inputs should produce null scores
                assertThat(scoreBlock.isNull(curPos), equalTo(true));
            } else {
                // Verify score is present
                assertFalse(scoreBlock.isNull(curPos));
                double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(curPos));

                // For multi-valued positions, the score should be the max of all input scores
                // (see RerankOutputBuilder.appendResponseToBlock)
                double expectedScore = inputTexts.stream().mapToDouble(this::makeScore).max().orElseThrow();

                assertThat(score, closeTo(expectedScore, 0.0001));
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
            if (Strings.hasText(text)) {
                inputs.add(text);
            }
        }

        return inputs;
    }

    /**
     * Tests reranking with multiple input fields. Values from all input channels
     * are combined and sent to the inference service.
     */
    public void testMultipleInputFields() throws Exception {
        // Use two different input channels
        int channel1 = 0;
        int channel2 = 1;

        // Save original channels and set up multi-field
        List<Integer> originalChannels = inputChannels;
        inputChannels = List.of(channel1, channel2);

        try (
            BytesRefBlock.Builder blockBuilder1 = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder blockBuilder2 = blockFactory().newBytesRefBlockBuilder(3)
        ) {
            // Block 1:
            // Position 0: single value "a"
            blockBuilder1.appendBytesRef(new BytesRef("a"));
            // Position 1: two values ["b1", "b2"]
            blockBuilder1.beginPositionEntry();
            blockBuilder1.appendBytesRef(new BytesRef("b1"));
            blockBuilder1.appendBytesRef(new BytesRef("b2"));
            blockBuilder1.endPositionEntry();
            // Position 2: null
            blockBuilder1.appendNull();
            // Position 3: single value
            blockBuilder1.appendBytesRef(new BytesRef("c"));

            // Block 2:
            // Position 0: two values ["x1", "x2"]
            blockBuilder2.beginPositionEntry();
            blockBuilder2.appendBytesRef(new BytesRef("x1"));
            blockBuilder2.appendBytesRef(new BytesRef("x2"));
            blockBuilder2.endPositionEntry();
            // Position 1: single value "y"
            blockBuilder2.appendBytesRef(new BytesRef("y"));
            // Position 2: null
            blockBuilder2.appendNull();
            // Position 3: null
            blockBuilder2.appendNull();

            var runner = new TestDriverRunner().builder(driverContext()).input(blockBuilder1.build(), blockBuilder2.build());

            try {
                // Create a simple factory with multiple input evaluators
                Operator.OperatorFactory factory = new RerankOperator.Factory(
                    mockedInferenceService(),
                    SIMPLE_INFERENCE_ID,
                    QUERY_TEXT,
                    inputChannels.stream().map(this::evaluatorFactory).toList(),
                    2,
                    BATCH_SIZE
                );

                // Verify factory is created correctly
                assertNotNull(factory);

                List<Page> results = runner.run(factory);
                assertThat(results, hasSize(1));
                Page resultPage = results.get(0);

                assertThat(resultPage.getPositionCount(), equalTo(4));
                assertThat(resultPage.getBlockCount(), equalTo(3)); // original + score

                // get the score channel
                DoubleBlock scoreBlock = resultPage.getBlock(2);

                // Position 0
                assertFalse(scoreBlock.isNull(0));
                double expectedScore0 = makeScore("a", "x1", "x2");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), closeTo(expectedScore0, 0.0001));

                // Position 1
                assertFalse(scoreBlock.isNull(1));
                double expectedScore1 = makeScore("b1", "b2", "y");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), closeTo(expectedScore1, 0.0001));

                // Position 2: null - should produce null score
                assertTrue(scoreBlock.isNull(2));

                // Position 3
                assertFalse(scoreBlock.isNull(3));
                double expectedScore3 = makeScore("c");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(3)), closeTo(expectedScore3, 0.0001));

            } finally {
                // Restore original channels
                inputChannels = originalChannels;
            }
        }

        allBreakersEmpty();
    }

    /**
     * Tests reranking with multi-valued fields. When a position has multiple values,
     * all values are sent to the inference service, and the max score is returned.
     */
    public void testMultiValuedFields() throws Exception {
        final int positionCount = 4;

        // Create a page with multi-valued BytesRef blocks
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(positionCount)) {
            // Position 0: single value
            builder.appendBytesRef(new BytesRef("single_value"));

            // Position 1: two values - should get max score
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("multi_a"));
            builder.appendBytesRef(new BytesRef("multi_b"));
            builder.endPositionEntry();

            // Position 2: null
            builder.appendNull();

            // Position 3: three values - should get max score
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("triple_x"));
            builder.appendBytesRef(new BytesRef("triple_y"));
            builder.appendBytesRef(new BytesRef("triple_z"));
            builder.endPositionEntry();

            var runner = new TestDriverRunner().builder(driverContext()).input(builder.build());

            // Create operator with a single input channel (channel 0)
            List<Integer> savedChannels = inputChannels;
            inputChannels = List.of(0);
            int savedScoreChannel = scoreChannel;
            scoreChannel = 1; // Append score as new column

            try {
                Operator.OperatorFactory factory = new RerankOperator.Factory(
                    mockedInferenceService(),
                    SIMPLE_INFERENCE_ID,
                    QUERY_TEXT,
                    inputChannels.stream().map(this::evaluatorFactory).toList(),
                    scoreChannel,
                    BATCH_SIZE
                );

                List<Page> results = runner.run(factory);
                assertThat(results, hasSize(1));
                Page resultPage = results.get(0);

                assertThat(resultPage.getPositionCount(), equalTo(positionCount));
                assertThat(resultPage.getBlockCount(), equalTo(2)); // original + score

                DoubleBlock scoreBlock = resultPage.getBlock(scoreChannel);

                // Position 0: single value - score based on "single_value"
                assertFalse(scoreBlock.isNull(0));
                double expectedScore0 = makeScore("single_value");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), closeTo(expectedScore0, 0.0001));

                // Position 1: multi-valued - max score of "multi_a" and "multi_b"
                assertFalse(scoreBlock.isNull(1));
                double expectedScore1 = makeScore("multi_a", "multi_b");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(1)), closeTo(expectedScore1, 0.0001));

                // Position 2: null - should produce null score
                assertTrue(scoreBlock.isNull(2));

                // Position 3: multi-valued - max score of three values
                assertFalse(scoreBlock.isNull(3));
                double expectedScore3 = makeScore("triple_x", "triple_y", "triple_z");
                assertThat(scoreBlock.getDouble(scoreBlock.getFirstValueIndex(3)), closeTo(expectedScore3, 0.0001));

                // Clean up result pages
                results.forEach(Page::releaseBlocks);
            } finally {
                inputChannels = savedChannels;
                scoreChannel = savedScoreChannel;
            }
        }
    }

    @Override
    protected RankedDocsResults mockInferenceResult(InferenceAction.Request request) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        List<String> inputs = request.getInput();

        for (int i = 0; i < inputs.size(); i++) {
            String input = inputs.get(i);
            rankedDocs.add(new RankedDocsResults.RankedDoc(i, makeScore(input), input));
        }

        return new RankedDocsResults(rankedDocs);
    }

    private float makeScore(String... textValues) {
        float maxHash = Integer.MIN_VALUE;
        for (String text : textValues) {
            if (Strings.hasText(text)) {
                int hash = text.hashCode();
                if (hash > maxHash) {
                    maxHash = hash;
                }
            }
        }

        return maxHash / Integer.MAX_VALUE;
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "RerankOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "], query=[" + QUERY_TEXT + "], score_channel=[" + scoreChannel + "]]"
        );
    }

    public void testInferenceFailure() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        Exception expectedException = new ElasticsearchException("Inference service unavailable");
        InferenceService failingService = mockedInferenceService(shouldFail, expectedException);

        Operator.OperatorFactory factory = new RerankOperator.Factory(
            failingService,
            SIMPLE_INFERENCE_ID,
            QUERY_TEXT,
            List.of(evaluatorFactory(inputChannel)),
            scoreChannel,
            BATCH_SIZE
        );

        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(simpleInput(runner.context().blockFactory(), between(1, 100)));
        Exception actualException = expectThrows(ElasticsearchException.class, () -> runner.run(factory));

        assertThat(actualException.getMessage(), equalTo("Inference service unavailable"));
    }
}
