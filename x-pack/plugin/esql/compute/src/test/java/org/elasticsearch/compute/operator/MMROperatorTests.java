/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.BitSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MMROperatorTests extends OperatorTestCase {

    static List<float[]> TEST_VECTORS = List.of(
        new float[] { 0.4f, 0.2f, 0.4f, 0.4f },
        new float[] { 0.4f, 0.2f, 0.3f, 0.3f },
        new float[] { 0.4f, 0.1f, 0.3f, 0.3f },
        new float[] { 0.1f, 0.9f, 0.5f, 0.9f },
        new float[] { 0.1f, 0.9f, 0.5f, 0.9f },
        new float[] { 0.05f, 0.05f, 0.05f, 0.05f }
    );

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 8192) {

            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                var blocks = new Block[3];
                DocBlock.Builder docBuilder = null;
                float[] vectors = new float[length * 4];
                int[] vectorPositions = new int[length + 1];
                int vectorIndex = 0;
                try {
                    docBuilder = DocBlock.newBlockBuilder(blockFactory, length);
                    for (int i = 1; i <= length; i++) {
                        docBuilder.appendDoc(i + positionOffset);
                        docBuilder.appendShard(0);
                        docBuilder.appendSegment(0);

                        // todo - use random vector of length
                        var thisVector = TEST_VECTORS.get(randomIntBetween(0, TEST_VECTORS.size() - 1));
                        for (int v = 0; v < 4; v++) {
                            vectors[vectorIndex + v] = thisVector[v];
                        }
                        vectorPositions[i - 1] = vectorIndex;
                        vectorIndex += 4;
                    }
                    vectorPositions[length] = vectorIndex;

                    blocks[0] = docBuilder.build();

                    blocks[1] = blockFactory.newFloatArrayBlock(vectors, length, vectorPositions, new BitSet(), Block.MvOrdering.UNORDERED);

                    // score block
                    var scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(length);
                    for (int i = 0; i < length; i++) {
                        scoreBlockBuilder.appendDouble(1.0);
                    }
                    blocks[2] = scoreBlockBuilder.build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    if (docBuilder != null) {
                        docBuilder.close();
                    }
                }

                finish();
                return new Page(blocks);
            }
        };
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        var stopHere = true;
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new MMROperator.Factory(0, "vector_field", 1, 5, null, null, 2);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        String description = "MMROperator[diversificationField="
            + "(docIDChannel="
            + 0
            + "), diversificationField="
            + "vector_field"
            + " (channel="
            + 1
            + "), limit="
            + 5
            + ", queryVector="
            + "null"
            + ", lambda="
            + "null"
            + "]";
        return equalTo(description);
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
