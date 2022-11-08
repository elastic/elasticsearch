/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ConstantIntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;

public class ProjectOperatorTests extends ESTestCase {

    public void testProjectionOnEmptyPage() throws Exception {
        var page = new Page(0);
        var projection = new ProjectOperator(randomMask(randomIntBetween(2, 10)));
        projection.addInput(page);
        assertEquals(page, projection.getOutput());
    }

    public void testProjection() throws Exception {
        var size = randomIntBetween(2, 5);
        var blocks = new Block[size];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new ConstantIntBlock(i, size);
        }

        var page = new Page(size, blocks);
        var mask = randomMask(size);

        var projection = new ProjectOperator(mask);
        projection.addInput(page);
        var out = projection.getOutput();
        assertEquals(mask.cardinality(), out.getBlockCount());

        int lastSetIndex = -1;
        for (int i = 0; i < out.getBlockCount(); i++) {
            var block = out.getBlock(i);
            var shouldBeSetInMask = block.getInt(0);
            assertTrue(mask.get(shouldBeSetInMask));
            lastSetIndex = mask.nextSetBit(lastSetIndex + 1);
            assertEquals(shouldBeSetInMask, lastSetIndex);
        }
    }

    private BitSet randomMask(int size) {
        var mask = new BitSet(size);
        for (int i = 0; i < size; i++) {
            mask.set(i, randomBoolean());
        }
        return mask;
    }
}
