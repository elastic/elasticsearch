/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;

public class RowInTableLookupOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomFrom(1, 7, 14, 20)));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertSimpleOutput(input, results, 0, 1);
    }

    private void assertSimpleOutput(List<Page> input, List<Page> results, int keyChannel, int outputChannel) {
        int count = input.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(count));
        int keysIdx = 0;
        int ordsIdx = 0;
        LongBlock keys = null;
        int keysOffset = 0;
        IntBlock ords = null;
        int ordsOffset = 0;
        int p = 0;
        while (p < count) {
            if (keys == null) {
                keys = input.get(keysIdx++).getBlock(keyChannel);
            }
            if (ords == null) {
                ords = results.get(ordsIdx++).getBlock(outputChannel);
            }
            int valueCount = keys.getValueCount(p - keysOffset);
            assertThat(ords.getValueCount(p - ordsOffset), equalTo(valueCount));
            int keysStart = keys.getFirstValueIndex(p - keysOffset);
            int ordsStart = ords.getFirstValueIndex(p - ordsOffset);
            for (int k = keysStart, l = ordsStart; k < keysStart + valueCount; k++, l++) {
                assertThat(ords.getInt(l), equalTo(switch ((int) keys.getLong(k)) {
                    case 1 -> 0;
                    case 7 -> 1;
                    case 14 -> 2;
                    case 20 -> 3;
                    default -> null;
                }));
            }
            p++;
            if (p - keysOffset == keys.getPositionCount()) {
                keysOffset += keys.getPositionCount();
                keys = null;
            }
            if (p - ordsOffset == ords.getPositionCount()) {
                ordsOffset += ords.getPositionCount();
                ords = null;
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new RowInTableLookupOperator.Factory(
            new RowInTableLookupOperator.Key[] {
                new RowInTableLookupOperator.Key(
                    "foo",
                    TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock()
                ) },
            new int[] { 0 }
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return matchesRegex("RowInTableLookup\\[keys=\\[\\{name=foo, type=LONG, positions=4, size=\\d+b}], mapping=\\[0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return matchesRegex(
            "RowInTableLookup\\[PackedValuesBlockHash\\{groups=\\[0:LONG], entries=4, size=\\d+b}, keys=\\[foo], mapping=\\[0]]"
        );
    }

    public void testSelectBlocks() {
        DriverContext context = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(
            new TupleBlockSourceOperator(
                context.blockFactory(),
                LongStream.range(0, 1000).mapToObj(l -> Tuple.tuple(randomLong(), randomFrom(1L, 7L, 14L, 20L)))
            )
        );
        List<Page> clonedInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(
            new RowInTableLookupOperator.Factory(
                new RowInTableLookupOperator.Key[] {
                    new RowInTableLookupOperator.Key(
                        "foo",
                        TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 1, 7, 14, 20 }, 4).asBlock()
                    ) },
                new int[] { 1 }
            ).get(context),
            input.iterator(),
            context
        );
        assertSimpleOutput(clonedInput, results, 1, 2);
    }
}
