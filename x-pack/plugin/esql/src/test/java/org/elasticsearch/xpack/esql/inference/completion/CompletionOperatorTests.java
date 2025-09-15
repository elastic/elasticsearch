/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CompletionOperatorTests extends InferenceOperatorTestCase<ChatCompletionResults> {
    private static final String SIMPLE_INFERENCE_ID = "test_completion";

    private int inputChannel;

    @Before
    public void initCompletionChannels() {
        inputChannel = between(0, inputsCount - 1);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CompletionOperator.Factory(mockedInferenceService(), SIMPLE_INFERENCE_ID, evaluatorFactory(inputChannel));
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
        BytesRefBlock inputBlock = resultPage.getBlock(inputChannel);
        BytesRefBlock resultBlock = resultPage.getBlock(inputPage.getBlockCount());

        BlockStringReader blockReader = new InferenceOperatorTestCase.BlockStringReader();

        for (int curPos = 0; curPos < inputPage.getPositionCount(); curPos++) {
            if (inputBlock.isNull(curPos)) {
                assertThat(resultBlock.isNull(curPos), equalTo(true));
            } else {
                assertThat(
                    blockReader.readString(resultBlock, curPos),
                    equalTo(blockReader.readString(inputBlock, curPos).toUpperCase(java.util.Locale.ROOT))
                );
            }
        }
    }

    @Override
    protected ChatCompletionResults mockInferenceResult(InferenceAction.Request request) {
        List<ChatCompletionResults.Result> results = new ArrayList<>();
        for (String input : request.getInput()) {
            results.add(new ChatCompletionResults.Result(input.toUpperCase(Locale.ROOT)));
        }
        return new ChatCompletionResults(results);
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
