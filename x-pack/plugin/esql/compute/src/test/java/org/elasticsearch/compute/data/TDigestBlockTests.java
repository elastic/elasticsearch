/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import static org.hamcrest.Matchers.equalTo;

public class TDigestBlockTests extends ComputeTestCase {

    public void testComponentAccess() {
        TDigestBlock block;
        if (randomBoolean()) {
            block = randomBlockWithNulls();
        } else {
            block = (TDigestBlock) blockFactory().newConstantNullBlock(randomIntBetween(1, 100));
        }
        for (HistogramBlock.Component component : HistogramBlock.Component.values()) {
            Block componentBlock = block.buildHistogramComponentBlock(component);
            assertThat(componentBlock.getPositionCount(), equalTo(block.getPositionCount()));
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    assertThat(componentBlock.isNull(i), equalTo(true));
                } else {
                    TDigestHolder histo = block.getTDigestHolder(i);
                    switch (component) {
                        case MIN -> {
                            double expectedMin = histo.getMin();
                            if (Double.isNaN(expectedMin)) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(expectedMin));
                            }
                        }
                        case MAX -> {
                            double expectedMax = histo.getMax();
                            if (Double.isNaN(expectedMax)) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(expectedMax));
                            }
                        }
                        case SUM -> {
                            if (histo.getSum() == 0) {
                                assertThat(componentBlock.isNull(i), equalTo(true));
                            } else {
                                assertThat(componentBlock.getValueCount(i), equalTo(1));
                                int valueIndex = componentBlock.getFirstValueIndex(i);
                                assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo(histo.getSum()));
                            }
                        }
                        case COUNT -> {
                            assertThat(componentBlock.getValueCount(i), equalTo(1));
                            int valueIndex = componentBlock.getFirstValueIndex(i);
                            assertThat(((DoubleBlock) componentBlock).getDouble(valueIndex), equalTo((double) histo.getValueCount()));
                        }
                    }
                }
            }
            Releasables.close(componentBlock);
        }
        Releasables.close(block);
    }

    private TDigestBlock randomBlockWithNulls() {
        int elementCount = randomIntBetween(0, 100);
        TDigestBlockBuilder builder = blockFactory().newTDigestBlockBuilder(elementCount);
        for (int i = 0; i < elementCount; i++) {
            if (randomBoolean()) {
                builder.appendNull();
            } else {
                builder.append(BlockTestUtils.randomTDigest());
            }
        }
        return builder.build();
    }
}
