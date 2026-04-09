/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MvPercentileSimpleTests extends ESTestCase {
    public void testDoubleMvAsc() {
        try (DoubleBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendDouble(80);
            builder.appendDouble(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendDouble(-6.33);
            builder.appendDouble(-3.34);
            builder.appendDouble(-0.31);
            builder.appendDouble(6.23);
            builder.endPositionEntry();
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            try (DoubleBlock block = builder.build()) {
                MvPercentile.DoubleSortingScratch scratch = new MvPercentile.DoubleSortingScratch();
                double p0 = MvPercentile.calculateDoublePercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                double p1 = MvPercentile.calculateDoublePercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87.5));
                assertThat(p1, equalTo(1.325));
            }
        }
    }

    public void testDoubleRandomOrder() {
        try (DoubleBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newDoubleBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendDouble(80);
            builder.appendDouble(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendDouble(-3.34);
            builder.appendDouble(-6.33);
            builder.appendDouble(6.23);
            builder.appendDouble(-0.31);
            builder.endPositionEntry();
            try (DoubleBlock block = builder.build()) {
                MvPercentile.DoubleSortingScratch scratch = new MvPercentile.DoubleSortingScratch();
                double p0 = MvPercentile.calculateDoublePercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                double p1 = MvPercentile.calculateDoublePercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87.5));
                assertThat(p1, equalTo(1.325));
            }
        }
    }

    public void testIntMvAsc() {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendInt(80);
            builder.appendInt(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendInt(-6);
            builder.appendInt(-3);
            builder.appendInt(0);
            builder.appendInt(6);
            builder.endPositionEntry();
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            try (IntBlock block = builder.build()) {
                MvPercentile.IntSortingScratch scratch = new MvPercentile.IntSortingScratch();
                int p0 = MvPercentile.calculateIntPercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                int p1 = MvPercentile.calculateIntPercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87));
                assertThat(p1, equalTo(1));
            }
        }
    }

    public void testIntRandomOrder() {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendInt(80);
            builder.appendInt(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendInt(-3);
            builder.appendInt(-6);
            builder.appendInt(6);
            builder.appendInt(0);
            builder.endPositionEntry();
            try (IntBlock block = builder.build()) {
                MvPercentile.IntSortingScratch scratch = new MvPercentile.IntSortingScratch();
                int p0 = MvPercentile.calculateIntPercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                int p1 = MvPercentile.calculateIntPercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87));
                assertThat(p1, equalTo(1));
            }
        }
    }

    public void testLongMvAsc() {
        try (LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendLong(80);
            builder.appendLong(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(-6);
            builder.appendLong(-3);
            builder.appendLong(0);
            builder.appendLong(6);
            builder.endPositionEntry();
            builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
            try (LongBlock block = builder.build()) {
                MvPercentile.LongSortingScratch scratch = new MvPercentile.LongSortingScratch();
                long p0 = MvPercentile.calculateLongPercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                long p1 = MvPercentile.calculateLongPercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87L));
                assertThat(p1, equalTo(1L));
            }
        }
    }

    public void testLongRandomOrder() {
        try (LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(10)) {
            builder.beginPositionEntry();
            builder.appendLong(80);
            builder.appendLong(90);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(-3);
            builder.appendLong(-6);
            builder.appendLong(6);
            builder.appendLong(0);
            builder.endPositionEntry();
            try (LongBlock block = builder.build()) {
                MvPercentile.LongSortingScratch scratch = new MvPercentile.LongSortingScratch();
                long p0 = MvPercentile.calculateLongPercentile(block, block.getFirstValueIndex(0), block.getValueCount(0), 75, scratch);
                long p1 = MvPercentile.calculateLongPercentile(block, block.getFirstValueIndex(1), block.getValueCount(1), 75, scratch);
                assertThat(p0, equalTo(87L));
                assertThat(p1, equalTo(1L));
            }
        }
    }
}
