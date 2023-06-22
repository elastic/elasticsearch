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
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MergePositionsOperatorTests extends ESTestCase {

    public void testSimple() {
        IntVector positions = new IntArrayVector(new int[] { 2, 3, 5, 1 }, 4);
        BytesRefBlock inField1 = BytesRefBlock.newBlockBuilder(4)
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("a1"))
            .appendBytesRef(new BytesRef("c1"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("f5"))
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("r2"))
            .appendBytesRef(new BytesRef("k2"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("w0"))
            .build();
        IntBlock inField2 = IntBlock.newBlockBuilder(4).appendNull().appendInt(2020).appendInt(2023).appendNull().build();
        MergePositionsOperator mergeOperator = new MergePositionsOperator(7, new int[] { 1, 2 });
        mergeOperator.addInput(new Page(positions.asBlock(), inField1, inField2));
        mergeOperator.finish();
        Page out = mergeOperator.getOutput();
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(7));
        assertThat(out.getBlockCount(), equalTo(2));
        BytesRefBlock f1 = out.getBlock(0);
        IntBlock f2 = out.getBlock(1);

        assertTrue(f1.isNull(0));
        assertThat(BlockUtils.toJavaObject(f1, 1), equalTo(new BytesRef("w0")));
        assertThat(BlockUtils.toJavaObject(f1, 2), equalTo(List.of(new BytesRef("a1"), new BytesRef("c1"))));
        assertThat(BlockUtils.toJavaObject(f1, 3), equalTo(new BytesRef("f5")));
        assertTrue(f1.isNull(4));
        assertThat(BlockUtils.toJavaObject(f1, 5), equalTo(List.of(new BytesRef("r2"), new BytesRef("k2"))));
        assertTrue(f1.isNull(6));

        assertTrue(f2.isNull(0));
        assertTrue(f2.isNull(1));
        assertTrue(f2.isNull(2));
        assertThat(BlockUtils.toJavaObject(f2, 3), equalTo(2020));
        assertTrue(f2.isNull(4));
        assertThat(BlockUtils.toJavaObject(f2, 5), equalTo(2023));
        assertTrue(f2.isNull(6));
    }

    public void testMultiValues() {
        IntVector positions = new IntArrayVector(new int[] { 2, 3, 5, 1, 2 }, 5);
        BytesRefBlock inField1 = BytesRefBlock.newBlockBuilder(4)
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("a1"))
            .appendBytesRef(new BytesRef("c1"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("f5"))
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("r2"))
            .appendBytesRef(new BytesRef("k2"))
            .endPositionEntry()
            .appendBytesRef(new BytesRef("w0"))
            .beginPositionEntry()
            .appendBytesRef(new BytesRef("k1"))
            .appendBytesRef(new BytesRef("k2"))
            .endPositionEntry()
            .build();
        IntBlock inField2 = IntBlock.newBlockBuilder(5).appendNull().appendInt(2020).appendInt(2023).appendNull().appendInt(2021).build();
        MergePositionsOperator mergeOperator = new MergePositionsOperator(7, new int[] { 1, 2 });
        mergeOperator.addInput(new Page(positions.asBlock(), inField1, inField2));
        mergeOperator.finish();
        Page out = mergeOperator.getOutput();
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(7));
        assertThat(out.getBlockCount(), equalTo(2));
        BytesRefBlock f1 = out.getBlock(0);
        IntBlock f2 = out.getBlock(1);

        assertTrue(f1.isNull(0));
        assertThat(BlockUtils.toJavaObject(f1, 1), equalTo(new BytesRef("w0")));
        assertThat(
            BlockUtils.toJavaObject(f1, 2),
            equalTo(List.of(new BytesRef("a1"), new BytesRef("c1"), new BytesRef("k1"), new BytesRef("k2")))
        );
        assertThat(BlockUtils.toJavaObject(f1, 3), equalTo(new BytesRef("f5")));
        assertTrue(f1.isNull(4));
        assertThat(BlockUtils.toJavaObject(f1, 5), equalTo(List.of(new BytesRef("r2"), new BytesRef("k2"))));
        assertTrue(f1.isNull(6));

        assertTrue(f2.isNull(0));
        assertTrue(f2.isNull(1));
        assertThat(BlockUtils.toJavaObject(f2, 2), equalTo(2021));
        assertThat(BlockUtils.toJavaObject(f2, 3), equalTo(2020));
        assertTrue(f2.isNull(4));
        assertThat(BlockUtils.toJavaObject(f2, 5), equalTo(2023));
        assertTrue(f2.isNull(6));
    }
}
