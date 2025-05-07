/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CompositeBlockTests extends ComputeTestCase {

    static List<ElementType> supportedSubElementTypes = Arrays.stream(ElementType.values())
        .filter(
            e -> e != ElementType.COMPOSITE && e != ElementType.UNKNOWN && e != ElementType.DOC && e != ElementType.AGGREGATE_METRIC_DOUBLE
        )
        .toList();

    public static CompositeBlock randomCompositeBlock(BlockFactory blockFactory, int numBlocks, int positionCount) {
        Block[] blocks = new Block[numBlocks];
        for (int b = 0; b < numBlocks; b++) {
            ElementType elementType = randomFrom(supportedSubElementTypes);
            blocks[b] = RandomBlock.randomBlock(
                blockFactory,
                elementType,
                positionCount,
                elementType == ElementType.NULL || randomBoolean(),
                0,
                between(1, 2),
                0,
                between(1, 2)
            ).block();
        }
        return new CompositeBlock(blocks);
    }

    public void testFilter() {
        final BlockFactory blockFactory = blockFactory();
        int numBlocks = randomIntBetween(1, 1000);
        int positionCount = randomIntBetween(1, 1000);
        try (CompositeBlock origComposite = randomCompositeBlock(blockFactory, numBlocks, positionCount)) {
            int[] selected = new int[randomIntBetween(0, positionCount * 3)];
            for (int i = 0; i < selected.length; i++) {
                selected[i] = randomIntBetween(0, positionCount - 1);
            }
            try (CompositeBlock filteredComposite = origComposite.filter(selected)) {
                assertThat(filteredComposite.getBlockCount(), equalTo(numBlocks));
                assertThat(filteredComposite.getPositionCount(), equalTo(selected.length));
                for (int b = 0; b < numBlocks; b++) {
                    try (Block filteredSub = origComposite.getBlock(b).filter(selected)) {
                        assertThat(filteredComposite.getBlock(b), equalTo(filteredSub));
                    }
                }
            }
        }
    }
}
