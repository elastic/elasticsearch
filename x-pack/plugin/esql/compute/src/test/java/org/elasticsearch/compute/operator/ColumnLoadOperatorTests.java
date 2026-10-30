/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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

    public void testFactorySharesOneCopy() {
        BlockFactory blockFactory = blockFactory();
        CircuitBreaker breaker = blockFactory.breaker();
        Operator.OperatorFactory factory = simple();
        long before = breaker.getUsed();

        Operator first = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        long withCopy = breaker.getUsed();
        assertThat(withCopy, greaterThan(before));

        Operator second = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        assertThat("the second operator reuses the copy", breaker.getUsed(), equalTo(withCopy));

        first.close();
        assertThat("the copy stays while the second operator uses it", breaker.getUsed(), equalTo(withCopy));
        second.close();
        assertThat("the last close releases the copy", breaker.getUsed(), equalTo(before));

        // Once released, the next operator makes a fresh copy rather than using the closed one.
        Operator third = factory.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null));
        assertThat(breaker.getUsed(), equalTo(withCopy));
        third.close();
        assertThat(breaker.getUsed(), equalTo(before));
    }

    public void testTablesKeepCopyForNextFactory() {
        BlockFactory blockFactory = blockFactory();
        CircuitBreaker breaker = blockFactory.breaker();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        ColumnLoadOperator.Values values = new ColumnLoadOperator.Values(
            "values",
            TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 0, 3, 6, 9, 12 }, 5).asBlock()
        );
        long before = breaker.getUsed();
        try (HashJoinTables tables = new HashJoinTables()) {
            new ColumnLoadOperator.Factory(values, 0, tables).get(driverContext).close();
            long withCopy = breaker.getUsed();
            assertThat("the copy outlives the operators", withCopy, greaterThan(before));

            new ColumnLoadOperator.Factory(values, 0, tables).get(driverContext).close();
            assertThat("the next factory reuses the copy", breaker.getUsed(), equalTo(withCopy));
        }
        assertThat("closing the tables releases the copy", breaker.getUsed(), equalTo(before));
    }

    /**
     * The driver that made the copy can finish, closing its local breaker, while another driver still uses the copy.
     * Bytes released to a closed local breaker never reach its parent, so the copy must be charged to the parent.
     */
    public void testCopyOutlivesLocalBreaker() {
        BlockFactory blockFactory = blockFactory();
        Operator.OperatorFactory factory = simple();
        LocalCircuitBreaker firstBreaker = new LocalCircuitBreaker(blockFactory.breaker(), 0, Long.MAX_VALUE);
        LocalCircuitBreaker secondBreaker = new LocalCircuitBreaker(blockFactory.breaker(), 0, Long.MAX_VALUE);
        BlockFactory firstFactory = blockFactory.newChildFactory(firstBreaker);
        BlockFactory secondFactory = blockFactory.newChildFactory(secondBreaker);
        Operator first = factory.get(new DriverContext(firstFactory.bigArrays(), firstFactory, null));
        Operator second = factory.get(new DriverContext(secondFactory.bigArrays(), secondFactory, null));
        first.close();
        firstBreaker.close();
        second.close();
        secondBreaker.close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The values contain a null so they are an array block rather than a vector block: looking up from an array block
     * takes a reference to the shared copy itself, which is what concurrent drivers must not race on.
     */
    public void testConcurrentOperatorsShareCopy() {
        BlockFactory blockFactory = blockFactory();
        int valueCount = 10;
        LongBlock values;
        try (LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(valueCount)) {
            for (int i = 0; i < valueCount; i++) {
                if (i == 0) {
                    builder.appendNull();
                } else {
                    builder.appendLong(3L * i);
                }
            }
            values = builder.build();
        }
        Operator.OperatorFactory factory = new ColumnLoadOperator.Factory(new ColumnLoadOperator.Values("values", values), 0);
        int threads = between(2, 8);
        int pages = between(50, 200);
        startInParallel(threads, t -> {
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            try (Operator op = factory.get(driverContext)) {
                for (int p = 0; p < pages; p++) {
                    int size = between(1, 100);
                    int[] keys = new int[size];
                    for (int i = 0; i < size; i++) {
                        keys[i] = between(0, valueCount - 1);
                    }
                    op.addInput(new Page(blockFactory.newIntArrayVector(keys, size).asBlock()));
                    int checked = 0;
                    Page out;
                    while ((out = op.getOutput()) != null) {
                        try {
                            LongBlock loaded = out.getBlock(1);
                            for (int i = 0; i < loaded.getPositionCount(); i++) {
                                int key = keys[checked++];
                                if (key == 0) {
                                    assertTrue(loaded.isNull(i));
                                } else {
                                    assertThat(loaded.getLong(loaded.getFirstValueIndex(i)), equalTo(3L * key));
                                }
                            }
                        } finally {
                            out.releaseBlocks();
                        }
                    }
                    assertThat(checked, equalTo(size));
                }
            }
        });
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }
}
