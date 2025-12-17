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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceResponseItem;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CompletionOutputBuilderTests extends ComputeTestCase {

    public void testBuildSmallOutput() throws Exception {
        assertBuildOutput(between(1, 100));
    }

    public void testBuildLargeOutput() throws Exception {
        assertBuildOutput(between(10_000, 100_000));
    }

    public void testBuildOutputWithNulls() throws Exception {
        final int size = between(10, 100);
        final Page inputPage = randomInputPage(size, between(1, 20));

        CompletionOutputBuilder outputBuilder = new CompletionOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            // Mix null and non-null responses
            if (randomBoolean()) {
                // Null response with shape [0]
                responses.add(new BulkInferenceResponseItem(null, new int[] { 0 }, currentPos));
            } else {
                // Regular response with shape [1]
                ChatCompletionResults results = new ChatCompletionResults(
                    List.of(new ChatCompletionResults.Result("Completion result #" + currentPos))
                );
                InferenceAction.Response response = new InferenceAction.Response(results);
                responses.add(new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos));
            }
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            // Verify output content matches expected pattern
            BytesRefBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < responses.size(); pos++) {
                if (responses.get(pos).inferenceResponse() == null) {
                    assertTrue(outputBlock.isNull(pos));
                } else {
                    assertFalse(outputBlock.isNull(pos));
                }
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithMultiValuedResponses() throws Exception {
        final int size = 10;
        final Page inputPage = randomInputPage(size, 2);

        CompletionOutputBuilder outputBuilder = new CompletionOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = new ArrayList<>();

        for (int currentPos = 0; currentPos < inputPage.getPositionCount(); currentPos++) {
            int numResults = between(1, 5);
            List<ChatCompletionResults.Result> results = new ArrayList<>();
            for (int i = 0; i < numResults; i++) {
                results.add(new ChatCompletionResults.Result("Result " + currentPos + "-" + i));
            }

            ChatCompletionResults chatResults = new ChatCompletionResults(results);
            InferenceAction.Response response = new InferenceAction.Response(chatResults);
            responses.add(new BulkInferenceResponseItem(response, new int[] { numResults }, currentPos));
        }

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            // Verify multi-valued output
            BytesRefBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < responses.size(); pos++) {
                BulkInferenceResponseItem responseItem = responses.get(pos);
                int expectedCount = responseItem.shape()[0];
                assertThat(outputBlock.getValueCount(pos), equalTo(expectedCount));
            }
        }

        allBreakersEmpty();
    }

    public void testBuildOutputWithMixedShapes() throws Exception {
        final int size = 20;
        final Page inputPage = randomInputPage(size, 3);

        CompletionOutputBuilder outputBuilder = new CompletionOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = IntStream.range(0, inputPage.getPositionCount()).mapToObj(currentPos -> {
            int shapeValue = currentPos % 4;
            if (shapeValue == 0) {
                // Null response
                return new BulkInferenceResponseItem(null, new int[] { 0 }, currentPos);
            } else {
                // Multi-valued response
                List<ChatCompletionResults.Result> results = new ArrayList<>();
                for (int i = 0; i < shapeValue; i++) {
                    results.add(new ChatCompletionResults.Result("Value " + currentPos + "-" + i));
                }

                ChatCompletionResults chatResults = new ChatCompletionResults(results);
                InferenceAction.Response response = new InferenceAction.Response(chatResults);
                return (new BulkInferenceResponseItem(response, new int[] { shapeValue }, currentPos));
            }
        }).toList();
        ;

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));

            // Verify shapes
            BytesRefBlock outputBlock = outputPage.getBlock(outputPage.getBlockCount() - 1);
            for (int pos = 0; pos < responses.size(); pos++) {
                int expectedShape = responses.get(pos).shape()[0];
                if (expectedShape == 0) {
                    assertTrue(outputBlock.isNull(pos));
                } else {
                    assertFalse(outputBlock.isNull(pos));
                    assertThat(outputBlock.getValueCount(pos), equalTo(expectedShape));
                }
            }
        }

        allBreakersEmpty();
    }

    private void assertBuildOutput(int size) throws Exception {
        final Page inputPage = randomInputPage(size, between(1, 20));

        CompletionOutputBuilder outputBuilder = new CompletionOutputBuilder(blockFactory());
        List<BulkInferenceResponseItem> responses = IntStream.range(0, inputPage.getPositionCount()).mapToObj(currentPos -> {
            ;
            List<ChatCompletionResults.Result> results = List.of(new ChatCompletionResults.Result("Completion result #" + currentPos));
            InferenceAction.Response response = new InferenceAction.Response(new ChatCompletionResults(results));
            return new BulkInferenceResponseItem(response, new int[] { 1 }, currentPos);
        }).toList();

        try (Page outputPage = outputBuilder.buildOutputPage(inputPage, responses)) {
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
            assertOutputContent(outputPage.getBlock(outputPage.getBlockCount() - 1));

        }

        allBreakersEmpty();
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
                    RandomBlock.randomElementExcluding(List.of(ElementType.AGGREGATE_METRIC_DOUBLE)),
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
