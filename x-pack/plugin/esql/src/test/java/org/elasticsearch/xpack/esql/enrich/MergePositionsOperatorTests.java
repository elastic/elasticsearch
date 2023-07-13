/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MergePositionsOperatorTests extends ESTestCase {

    public void testSimple() {
        MergePositionsOperator mergeOperator = new MergePositionsOperator(
            randomBoolean(),
            7,
            0,
            new int[] { 1, 2 },
            new ElementType[] { ElementType.BYTES_REF, ElementType.INT }
        );
        mergeOperator.addInput(
            new Page(
                new ConstantIntVector(1, 1).asBlock(),
                BytesRefBlock.newBlockBuilder(1).appendBytesRef(new BytesRef("w0")).build(),
                IntBlock.newBlockBuilder(1).appendNull().build()
            )
        );
        mergeOperator.addInput(
            new Page(
                new ConstantIntVector(2, 1).asBlock(),
                BytesRefBlock.newBlockBuilder(1)
                    .beginPositionEntry()
                    .appendBytesRef(new BytesRef("a1"))
                    .appendBytesRef(new BytesRef("c1"))
                    .endPositionEntry()
                    .build(),
                IntBlock.newBlockBuilder(1).appendNull().build()
            )
        );
        mergeOperator.addInput(
            new Page(
                new ConstantIntVector(3, 2).asBlock(),
                BytesRefBlock.newBlockBuilder(1)
                    .appendBytesRef(new BytesRef("f5"))
                    .beginPositionEntry()
                    .appendBytesRef(new BytesRef("k1"))
                    .appendBytesRef(new BytesRef("k2"))
                    .endPositionEntry()
                    .build(),
                IntBlock.newBlockBuilder(1).appendInt(2020).appendInt(2021).build()
            )
        );
        mergeOperator.addInput(
            new Page(
                new ConstantIntVector(5, 1).asBlock(),
                BytesRefBlock.newBlockBuilder(1)
                    .beginPositionEntry()
                    .appendBytesRef(new BytesRef("r2"))
                    .appendBytesRef(new BytesRef("k2"))
                    .endPositionEntry()
                    .build(),
                IntBlock.newBlockBuilder(1).appendInt(2023).build()
            )
        );
        mergeOperator.finish();
        Page out = mergeOperator.getOutput();
        assertTrue(mergeOperator.isFinished());
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(7));
        assertThat(out.getBlockCount(), equalTo(2));
        BytesRefBlock f1 = out.getBlock(0);
        IntBlock f2 = out.getBlock(1);

        assertTrue(f1.isNull(0));
        assertThat(BlockUtils.toJavaObject(f1, 1), equalTo(new BytesRef("w0")));
        assertThat(BlockUtils.toJavaObject(f1, 2), equalTo(List.of(new BytesRef("a1"), new BytesRef("c1"))));
        assertThat(BlockUtils.toJavaObject(f1, 3), equalTo(List.of(new BytesRef("f5"), new BytesRef("k1"), new BytesRef("k2"))));
        assertTrue(f1.isNull(4));
        assertThat(BlockUtils.toJavaObject(f1, 5), equalTo(List.of(new BytesRef("r2"), new BytesRef("k2"))));
        assertTrue(f1.isNull(6));

        assertTrue(f2.isNull(0));
        assertTrue(f2.isNull(1));
        assertTrue(f2.isNull(2));
        assertThat(BlockUtils.toJavaObject(f2, 3), equalTo(List.of(2020, 2021)));
        assertTrue(f2.isNull(4));
        assertThat(BlockUtils.toJavaObject(f2, 5), equalTo(2023));
        assertTrue(f2.isNull(6));
    }
}
