/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NestedExpandOperatorTests extends NestedIndexOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        NestedChildren.Factory children = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("price", "note"),
            List.of(DataType.LONG, DataType.TEXT)
        );
        return new NestedExpandOperator.Factory(0, children, "items", List.of(DataType.LONG, DataType.TEXT), false);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<List<Object>> expected = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                int parent = parent(page, p);
                if (parent % 4 == 0) {
                    expected.add(Arrays.asList(parent, null, null));
                }
                for (int j = 0; j < parent % 4; j++) {
                    expected.add(List.of(parent, 10L * parent + j, "n" + j));
                }
            }
        }
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            LongBlock prices = page.getBlock(1);
            BytesRefBlock notes = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.add(
                    Arrays.asList(
                        parent(page, p),
                        prices.isNull(p) ? null : prices.getLong(prices.getFirstValueIndex(p)),
                        notes.isNull(p) ? null : notes.getBytesRef(notes.getFirstValueIndex(p), new BytesRef()).utf8ToString()
                    )
                );
            }
        }
        assertThat(actual, equalTo(expected));
    }

    /**
     * Without child fields and with {@code flatten}, no block grows with the children, so the positions of the child rows
     * are the only memory that does. They count against the breaker while the page expands, and are released after.
     */
    public void testChildRowPositionsCountAgainstTheBreaker() {
        RecordingBreaker breaker = new RecordingBreaker();
        BlockFactory blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        NestedChildren.Factory children = NestedChildren.factory(new IndexedByShardIdFromSingleton<>(shard), "items", List.of(), List.of());
        try (Operator operator = new NestedExpandOperator.Factory(0, children, "items", List.of(), true).get(context)) {
            operator.addInput(parents(blockFactory, 0, PARENTS));
            operator.getOutput().releaseBlocks();
        }
        // 1500 children, and one row for each of the 250 parents without children
        assertThat(breaker.added(NestedExpandOperator.BREAKER_LABEL), greaterThanOrEqualTo(1750L * Integer.BYTES));
        assertThat(breaker.used, equalTo(0L));
    }

    /** Records the bytes reserved under each label, and the bytes held in total. */
    private static class RecordingBreaker extends NoopCircuitBreaker {
        private final Map<String, Long> added = new HashMap<>();
        private long used;

        RecordingBreaker() {
            super("recording");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            added.merge(label, bytes, Long::sum);
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        long added(String label) {
            return added.getOrDefault(label, 0L);
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }
}
