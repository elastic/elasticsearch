/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * {@link PackedValuesBlockHash#add(Page, GroupingAggregatorFunction.AddInput, boolean)} with {@code skipMultivalues}.
 */
public class PackedValuesBlockHashSkipMultivaluesTests extends ESTestCase {
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * A row with a multivalued key gets a {@code null} group and adds no key, not even one for each combination of its values.
     * {@code [7, 7]} still counts as multivalued. A null key keeps its group, as without the option.
     */
    public void testSkipsMultivaluedRows() {
        Block[] keys = new Block[] {
            longs(row(1L), row(1L, 2L, 3L), row(2L), row(7L, 7L), row()),
            longs(row(10L), row(10L, 20L, 30L), row(20L), row(10L), row(10L)) };
        try (PackedValuesBlockHash hash = newHash()) {
            List<Integer> groups = add(hash, keys, true);
            assertThat(groups, equalTo(Arrays.asList(0, null, 1, null, 2)));
            assertThat(hash.numKeys(), equalTo(3));
        } finally {
            Releasables.close(keys);
        }
    }

    /**
     * Without the option, the same rows add a key for each combination of a multivalued row's values.
     */
    public void testWithoutSkipAddsEveryCombination() {
        Block[] keys = new Block[] { longs(row(1L), row(1L, 2L, 3L)), longs(row(10L), row(10L, 20L, 30L)) };
        try (PackedValuesBlockHash hash = newHash()) {
            add(hash, keys, false);
            assertThat(hash.numKeys(), equalTo(9));
        } finally {
            Releasables.close(keys);
        }
    }

    /**
     * Pages without a multivalued key give the same groups with and without the option.
     */
    public void testSameGroupsWithoutMultivalues() {
        Block[] keys = new Block[] { longs(row(1L), row(2L), row(), row(1L)), longs(row(10L), row(20L), row(10L), row(10L)) };
        try (PackedValuesBlockHash skipping = newHash(); PackedValuesBlockHash plain = newHash()) {
            assertThat(add(skipping, keys, true), equalTo(add(plain, keys, false)));
        } finally {
            Releasables.close(keys);
        }
    }

    private PackedValuesBlockHash newHash() {
        return BlockHash.buildPackedValuesBlockHash(
            List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.LONG)),
            blockFactory,
            between(1, 16)
        );
    }

    /**
     * Adds {@code keys} and returns each row's group, {@code null} for a row with no group.
     */
    private static List<Integer> add(PackedValuesBlockHash hash, Block[] keys, boolean skipMultivalues) {
        Integer[] groups = new Integer[keys[0].getPositionCount()];
        hash.add(new Page(keys), new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    groups[positionOffset + p] = groupIds.getInt(p);
                }
            }

            private void addBlock(int positionOffset, IntBlock groupIds) {
                for (int p = 0; p < groupIds.getPositionCount(); p++) {
                    groups[positionOffset + p] = groupIds.isNull(p) ? null : groupIds.getInt(groupIds.getFirstValueIndex(p));
                }
            }

            @Override
            public void close() {}
        }, skipMultivalues);
        return new ArrayList<>(Arrays.asList(groups));
    }

    private static long[] row(long... values) {
        return values;
    }

    private LongBlock longs(long[]... rows) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(rows.length)) {
            for (long[] row : rows) {
                if (row.length == 0) {
                    builder.appendNull();
                } else if (row.length == 1) {
                    builder.appendLong(row[0]);
                } else {
                    builder.beginPositionEntry();
                    for (long value : row) {
                        builder.appendLong(value);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
