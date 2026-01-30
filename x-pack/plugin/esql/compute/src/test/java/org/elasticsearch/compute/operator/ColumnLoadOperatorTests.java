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
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class ColumnLoadOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceIntBlockSourceOperator(blockFactory, IntStream.range(0, size).map(l -> between(0, 4)));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int count = input.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(count));
        int keysIdx = 0;
        int loadedIdx = 0;
        IntBlock keys = null;
        int keysOffset = 0;
        LongBlock loaded = null;
        int loadedOffset = 0;
        int p = 0;
        while (p < count) {
            if (keys == null) {
                keys = input.get(keysIdx++).getBlock(0);
            }
            if (loaded == null) {
                loaded = results.get(loadedIdx++).getBlock(1);
            }
            int valueCount = keys.getValueCount(p - keysOffset);
            assertThat(loaded.getValueCount(p - loadedOffset), equalTo(valueCount));
            int keysStart = keys.getFirstValueIndex(p - keysOffset);
            int loadedStart = loaded.getFirstValueIndex(p - loadedOffset);
            for (int k = keysStart, l = loadedStart; k < keysStart + valueCount; k++, l++) {
                assertThat(loaded.getLong(l), equalTo(3L * keys.getInt(k)));
            }
            p++;
            if (p - keysOffset == keys.getPositionCount()) {
                keysOffset += keys.getPositionCount();
                keys = null;
            }
            if (p - loadedOffset == loaded.getPositionCount()) {
                loadedOffset += loaded.getPositionCount();
                loaded = null;
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ColumnLoadOperator.Factory(
            new ColumnLoadOperator.Values(
                "values",
                TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 0, 3, 6, 9, 12 }, 5).asBlock()
            ),
            0
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ColumnLoad[values=values:LONG, positions=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
