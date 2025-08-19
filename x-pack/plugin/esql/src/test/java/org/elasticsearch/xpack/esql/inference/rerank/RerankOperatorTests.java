/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RerankOperatorTests extends InferenceOperatorTestCase<RankedDocsResults> {

    private static final String SIMPLE_INFERENCE_ID = "test_reranker";
    private static final String SIMPLE_QUERY = "query text";
    private int inputChannel;
    private int scoreChannel;

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RerankOperator.Factory(
            mockedInferenceService(),
            SIMPLE_INFERENCE_ID,
            SIMPLE_QUERY,
            evaluatorFactory(inputChannel),
            scoreChannel
        );
    }

    @Before
    public void initRerankChannels() {
        inputChannel = between(0, inputsCount - 1);
        scoreChannel = between(0, inputsCount - 1);
        if (scoreChannel == inputChannel) {
            scoreChannel++;
        }
    }

    @Override
    protected void assertSimpleOutput(List<Page> inputPages, List<Page> resultPages) {
        assertThat(inputPages, hasSize(resultPages.size()));

        for (int pageId = 0; pageId < inputPages.size(); pageId++) {
            Page inputPage = inputPages.get(pageId);
            Page resultPage = resultPages.get(pageId);

            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(Integer.max(scoreChannel + 1, inputPage.getBlockCount())));

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block resultBlock = resultPage.getBlock(channel);
                if (channel == scoreChannel) {
                    assertExpectedScore(inputPage.getBlock(inputChannel), (DoubleBlock) resultBlock);
                } else {
                    Block inputBlock = inputPage.getBlock(channel);
                    assertThat(resultBlock.getPositionCount(), equalTo(resultPage.getPositionCount()));
                    assertThat(resultBlock.elementType(), equalTo(inputBlock.elementType()));
                    assertBlockContentEquals(inputBlock, resultBlock);
                }
            }
        }
    }

    private void assertExpectedScore(BytesRefBlock inputBlock, DoubleBlock scoreBlock) {
        assertThat(scoreBlock.getPositionCount(), equalTo(inputBlock.getPositionCount()));
        BlockStringReader inputBlockReader = new InferenceOperatorTestCase.BlockStringReader();

        for (int pos = 0; pos < inputBlock.getPositionCount(); pos++) {
            if (inputBlock.isNull(pos)) {
                assertThat(scoreBlock.isNull(pos), equalTo(true));
            } else {
                double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(pos));
                double expectedScore = score(inputBlockReader.readString(inputBlock, pos));
                assertThat(score, equalTo(expectedScore));
            }
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "RerankOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "], query=[" + SIMPLE_QUERY + "], score_channel=[" + scoreChannel + "]]"
        );
    }

    @Override
    protected RankedDocsResults mockInferenceResult(InferenceAction.Request request) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        for (int rank = 0; rank < request.getInput().size(); rank++) {
            String inputText = request.getInput().get(rank);
            rankedDocs.add(new RankedDocsResults.RankedDoc(rank, score(inputText), inputText));
        }

        return new RankedDocsResults(rankedDocs);
    }

    private float score(String inputText) {
        return (float) inputText.hashCode() / 100;
    }
}
