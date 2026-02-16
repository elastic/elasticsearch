/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class FuseOperatorTestCase extends OperatorTestCase {
    protected int blocksCount;
    protected int discriminatorPosition;
    protected int scorePosition;
    protected int discriminatorCount;

    @Before
    public void initialize() {
        discriminatorPosition = randomIntBetween(1, 20);
        scorePosition = randomIntBetween(discriminatorPosition + 1, 50);
        blocksCount = randomIntBetween(scorePosition + 1, 100);
        discriminatorCount = randomIntBetween(1, 20);
    }

    protected void assertOutput(List<Page> input, List<Page> results, TriConsumer<String, Double, Double> assertScore) {
        assertEquals(input.size(), results.size());

        for (int i = 0; i < results.size(); i++) {
            Page resultPage = results.get(i);
            Page initialPage = input.get(i);

            assertEquals(initialPage.getPositionCount(), resultPage.getPositionCount());
            assertEquals(resultPage.getBlockCount(), blocksCount);

            BytesRefBlock discriminatorBlock = resultPage.getBlock(discriminatorPosition);
            DoubleVectorBlock actualScoreBlock = resultPage.getBlock(scorePosition);
            DoubleVectorBlock initialScoreBlock = initialPage.getBlock(scorePosition);

            for (int j = 0; j < resultPage.getPositionCount(); j++) {
                String discriminator = discriminatorBlock.getBytesRef(j, new BytesRef()).utf8ToString();
                double actualScore = actualScoreBlock.getDouble(j);
                double initialScore = initialScoreBlock.getDouble(j);
                assertScore.apply(discriminator, actualScore, initialScore);
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                Block[] blocks = new Block[blocksCount];

                try {
                    for (int b = 0; b < blocksCount; b++) {
                        if (b == scorePosition) {
                            try (var builder = blockFactory.newDoubleBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    builder.appendDouble(randomDoubleBetween(-1000, 1000, true));
                                }
                                blocks[b] = builder.build();
                            }
                        } else {
                            try (var builder = blockFactory.newBytesRefBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    String stringInput = b == discriminatorPosition
                                        ? "fork" + randomIntBetween(0, discriminatorCount)
                                        : randomAlphaOfLength(10);

                                    builder.appendBytesRef(new BytesRef(stringInput));
                                }
                                blocks[b] = builder.build();
                            }
                        }
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }

                currentPosition += length;
                return new Page(blocks);
            }
        };
    }

    protected Map<String, Double> randomWeights() {
        Map<String, Double> weights = new HashMap<>();
        for (int i = 0; i < discriminatorCount; i++) {
            if (randomBoolean()) {
                weights.put("fork" + i, randomDouble());
            }
        }
        return weights;
    }
}
