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
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CompletionOperatorTests extends InferenceOperatorTestCase<ChatCompletionResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_completion";

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CompletionOperator.Factory(mockedSimpleInferenceRunner(), SIMPLE_INFERENCE_ID, evaluatorFactory(0));
    }

    @Override
    protected ChatCompletionResults mockInferenceResult(InferenceAction.Request request) {
        return new ChatCompletionResults(
            request.getInput().stream().map(r -> new ChatCompletionResults.Result(r.toLowerCase(Locale.ROOT))).collect(Collectors.toList())
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));

        for (int curPage = 0; curPage < input.size(); curPage++) {
            Page inputPage = input.get(curPage);
            Page resultPage = results.get(curPage);

            assertEquals(inputPage.getPositionCount(), resultPage.getPositionCount());
            assertEquals(inputPage.getBlockCount() + 1, resultPage.getBlockCount());

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block inputBlock = inputPage.getBlock(channel);
                Block resultBlock = resultPage.getBlock(channel);
                assertBlockContentEquals(inputBlock, resultBlock);
            }

            assertCompletionResults(inputPage, resultPage);
        }
    }

    private void assertCompletionResults(Page inputPage, Page resultPage) {
        BytesRefBlock inputBlock = resultPage.getBlock(0);
        BytesRefBlock resultBlock = resultPage.getBlock(inputPage.getBlockCount());

        BytesRef scratch = new BytesRef();
        StringBuilder inputBuilder = new StringBuilder();

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            inputBuilder.setLength(0);
            int valueIndex = inputBlock.getFirstValueIndex(curPos);
            while (valueIndex < inputBlock.getFirstValueIndex(curPos) + inputBlock.getValueCount(curPos)) {
                scratch = inputBlock.getBytesRef(valueIndex, scratch);
                inputBuilder.append(scratch.utf8ToString());
                if (valueIndex < inputBlock.getValueCount(curPos) - 1) {
                    inputBuilder.append("\n");
                }
                valueIndex++;
            }
            scratch = resultBlock.getBytesRef(resultBlock.getFirstValueIndex(curPos), scratch);

            assertThat(scratch.utf8ToString(), equalTo(inputBuilder.toString().toLowerCase(Locale.ROOT)));
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("CompletionOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }
}
