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
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.elasticsearch.xpack.esql.inference.XContentRowEncoder;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RerankOperatorTests extends InferenceOperatorTestCase<RankedDocsResults> {

    private static final String SIMPLE_INFERENCE_ID = "test_reranker";
    private static final String SIMPLE_QUERY = "query text";

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Map<ColumnInfoImpl, EvalOperator.ExpressionEvaluator.Factory> fieldEvaluators = Map.of(
            new ColumnInfoImpl(randomIdentifier(), DataType.TEXT, List.of()),
            evaluatorFactory(0)
        );

        return new RerankOperator.Factory(
            mockedSimpleInferenceRunner(),
            SIMPLE_INFERENCE_ID,
            SIMPLE_QUERY,
            XContentRowEncoder.yamlRowEncoderFactory(fieldEvaluators),
            1
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> inputPages, List<Page> resultPages) {
        assertThat(inputPages, hasSize(resultPages.size()));

        for (int pageId = 0; pageId < inputPages.size(); pageId++) {
            Page inputPage = inputPages.get(pageId);
            Page resultPage = resultPages.get(pageId);

            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(Integer.max(2, inputPage.getBlockCount())));

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block inputBlock = inputPage.getBlock(channel);
                Block resultBlock = resultPage.getBlock(channel);

                assertThat(resultBlock.getPositionCount(), equalTo(resultPage.getPositionCount()));
                assertThat(resultBlock.elementType(), equalTo(inputBlock.elementType()));

                if (channel != 1) {
                    assertBlockContentEquals(inputBlock, resultBlock);
                }

                if (channel == 0) {
                    assertExpectedScore((BytesRefBlock) inputBlock, resultPage.getBlock(1));
                }
            }
        }
    }

    private void assertExpectedScore(BytesRefBlock inputBlock, DoubleBlock scoreBlock) {
        assertThat(scoreBlock.getPositionCount(), equalTo(inputBlock.getPositionCount()));
        for (int pos = 0; pos < inputBlock.getPositionCount(); pos++) {
            double score = scoreBlock.getDouble(scoreBlock.getFirstValueIndex(pos));
            double expectedScore = score(pos);
            assertThat(score, equalTo(expectedScore));
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "RerankOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "], query=[" + SIMPLE_QUERY + "], score_channel=[" + 1 + "]]"
        );
    }

    @Override
    protected RankedDocsResults mockInferenceResult(InferenceAction.Request request) {
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        for (int rank = 0; rank < request.getInput().size(); rank++) {
            rankedDocs.add(new RankedDocsResults.RankedDoc(rank, score(rank), request.getInput().get(rank)));
        }

        return new RankedDocsResults(rankedDocs);
    }

    private float score(int rank) {
        return 1f / (rank % 20);
    }
}
