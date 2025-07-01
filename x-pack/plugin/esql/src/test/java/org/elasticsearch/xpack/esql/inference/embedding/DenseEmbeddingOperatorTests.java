/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperatorTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.inference.embedding.DenseEmbeddingUtils.toBitArray;
import static org.elasticsearch.xpack.esql.inference.embedding.DenseEmbeddingUtils.toByteArray;
import static org.elasticsearch.xpack.esql.inference.embedding.DenseEmbeddingUtils.toFloatArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DenseEmbeddingOperatorTests extends InferenceOperatorTestCase<TextEmbeddingResults<?>> {

    private static final String SIMPLE_INFERENCE_ID = "test_dense_embedding";

    private static final String TEST_PARAMS_FORMATING = "dims=%s, embedding_type=%s";

    private final static List<Integer> DIMENSIONS = List.of(1, 32, 128, 512, 2048, 5096);
    private final static List<Class<? extends TextEmbeddingResults<?>>> EMBEDDING_TYPES = List.of(
        TextEmbeddingBitResults.class,
        TextEmbeddingByteResults.class,
        TextEmbeddingFloatResults.class
    );

    @ParametersFactory(argumentFormatting = TEST_PARAMS_FORMATING)
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] {});

        for (List<?> axis : List.of(DIMENSIONS, EMBEDDING_TYPES)) {

            List<Object[]> newParams = new ArrayList<>();
            for (Object[] combination : params) {
                for (Object element : axis) {
                    Object[] newCombination = new Object[combination.length + 1];
                    System.arraycopy(combination, 0, newCombination, 0, combination.length);
                    newCombination[newCombination.length - 1] = element;
                    newParams.add(newCombination);
                }
            }
            params = newParams;
        }

        return params;
    }

    private final int dimensions;
    private final Class<? extends TextEmbeddingResults<?>> embeddingType;

    public DenseEmbeddingOperatorTests(int dimensions, Class<? extends TextEmbeddingResults<?>> embeddingType) {
        this.dimensions = dimensions;
        this.embeddingType = embeddingType;
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new DenseEmbeddingOperator.Factory(mockedSimpleInferenceRunner(), dimensions, SIMPLE_INFERENCE_ID, evaluatorFactory(0));
    }

    @Override
    protected void assertSimpleOutput(List<Page> inputPages, List<Page> resultPages) {
        assertThat(inputPages, hasSize(resultPages.size()));

        for (int pageId = 0; pageId < inputPages.size(); pageId++) {
            Page inputPage = inputPages.get(pageId);
            Page resultPage = resultPages.get(pageId);

            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block inputBlock = inputPage.getBlock(channel);
                Block resultBlock = resultPage.getBlock(channel);

                assertThat(resultBlock.getPositionCount(), equalTo(resultPage.getPositionCount()));
                assertThat(resultBlock.elementType(), equalTo(inputBlock.elementType()));

                if (channel == 0) {
                    assertEmbeddingContent((BytesRefBlock) inputBlock, resultPage.getBlock(inputPage.getBlockCount()));
                }
            }
        }
    }

    private void assertEmbeddingContent(BytesRefBlock inputBlock, FloatBlock embeddingsBlock) {
        BytesRef scratch = new BytesRef();
        for (int position = 0; position < inputBlock.getPositionCount(); position++) {
            String textContent = "";
            if (inputBlock.isNull(position) == false) {
                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(position), scratch);
                textContent = BytesRefs.toString(scratch);
            }
            float[] expectedEmbedding = new float[dimensions];
            if (embeddingType.equals(TextEmbeddingFloatResults.class)) {
                expectedEmbedding = toFloatArray(textContent.length(), dimensions);
            }
            if (embeddingType.equals(TextEmbeddingByteResults.class) || embeddingType.equals(TextEmbeddingBitResults.class)) {
                byte[] bytesValues = embeddingType.equals(TextEmbeddingBitResults.class)
                    ? toBitArray(textContent.length(), dimensions)
                    : toByteArray(textContent.length(), dimensions);
                assertThat(bytesValues.length, equalTo(dimensions));
                for (int i = 0; i < bytesValues.length; i++) {
                    expectedEmbedding[i] = bytesValues[i];
                }
            }

            assertThat(embeddingsBlock.isNull(position), equalTo(false));
            assertThat(embeddingsBlock.getValueCount(position), equalTo(dimensions));
            for (int valueIndex = 0; valueIndex < dimensions; valueIndex++) {
                assertThat(
                    embeddingsBlock.getFloat(embeddingsBlock.getFirstValueIndex(position) + valueIndex),
                    equalTo(expectedEmbedding[valueIndex])
                );
            }
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("DenseEmbeddingOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "]]");
    }

    @Override
    protected TextEmbeddingResults<?> mockInferenceResult(InferenceAction.Request request) {
        return DenseEmbeddingUtils.inferenceResults(
            request.getInput().stream().mapToInt(String::length).toArray(),
            embeddingType,
            dimensions
        );
    }
}
