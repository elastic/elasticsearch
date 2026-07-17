/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongSwissHash;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Exercises {@link BlockHash.Router}, the bucket-sort-routing capability added for partitioned
 * hash aggregation (see scratch/partitioned-hash-aggregation-design.md, Phase 2 build order step
 * 2). Confirms {@code partitionHashOfRow} agrees with {@link LongSwissHash#hash}, that
 * {@code addRow} dedups/numbers groups exactly like the normal {@link BlockHash#add} path
 * (including reserving group {@code 0} for null), and that inserting through the router is
 * visible to and consistent with the normal add path on the same table.
 */
public class LongBlockHashRouterTests extends BlockHashTestCase {

    private BlockHash longBlockHash() {
        return BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)), blockFactory, 16 * 1024, true);
    }

    public void testRouterNullWhenSwissHashUnavailable() {
        assumeFalse("only relevant when SwissHash is unavailable on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        try (BlockHash hash = longBlockHash()) {
            assertThat(hash.router(), nullValue());
        }
    }

    public void testPartitionHashMatchesLongSwissHash() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        long[] values = { 5, -3, 0, Long.MAX_VALUE, Long.MIN_VALUE, 12345 };
        try (BlockHash hash = longBlockHash(); LongBlock keys = blockFactory.newLongArrayVector(values, values.length).asBlock()) {
            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());
            Page page = new Page(keys);
            for (int i = 0; i < values.length; i++) {
                assertThat(router.partitionHashOfRow(page, i), equalTo(LongSwissHash.hash(values[i])));
            }
        }
    }

    public void testAddRowDedupsAndReservesZeroForNull() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        long[] values = { 5, -3, 5, 0, -3, 100, 0 };
        try (BlockHash hash = longBlockHash(); LongBlock keys = blockFactory.newLongArrayVector(values, values.length).asBlock()) {
            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());

            Page page = new Page(keys);
            Map<Long, Integer> groupOf = new HashMap<>();
            for (int i = 0; i < values.length; i++) {
                int partitionHash = router.partitionHashOfRow(page, i);
                int groupId = router.addRow(page, i, partitionHash);
                assertThat("group id reserved for null must never be assigned to a real key", groupId, greaterThan(0));
                Integer existing = groupOf.putIfAbsent(values[i], groupId);
                if (existing != null) {
                    assertThat("repeated key must dedup to the same group id", groupId, equalTo(existing));
                }
            }
            assertThat("every distinct key gets a distinct group id", new HashSet<>(groupOf.values()), hasSize(groupOf.size()));

            int[] selectedArr = groupOf.values().stream().mapToInt(Integer::intValue).sorted().toArray();
            try (IntVector selected = blockFactory.newIntArrayVector(selectedArr, selectedArr.length)) {
                Block[] keysBack = hash.getKeys(selected);
                try {
                    LongBlock keyBlockBack = (LongBlock) keysBack[0];
                    Map<Integer, Long> reverse = new HashMap<>();
                    groupOf.forEach((k, g) -> reverse.put(g, k));
                    for (int i = 0; i < selectedArr.length; i++) {
                        assertThat(keyBlockBack.getLong(i), equalTo(reverse.get(selectedArr[i])));
                    }
                } finally {
                    Releasables.close(keysBack);
                }
            }
        }
    }

    public void testAddRowOnNullPinsToZeroAndMarksSeenNull() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        // A null key isn't hashed (there's nothing to hash), so the hash argument is a dummy value
        // here: addRow must detect the null itself and route it to the reserved group regardless.
        try (BlockHash hash = longBlockHash()) {
            LongBlock keys;
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(2)) {
                builder.appendNull();
                builder.appendLong(5);
                keys = builder.build();
            }
            try (keys) {
                Page page = new Page(keys);
                BlockHash.Router router = hash.router();
                assertThat(router, notNullValue());
                assertThat(router.addRow(page, 0, 0), equalTo(0));
                assertThat(router.addRow(page, 0, 0), equalTo(0));

                int nonNullGroup = router.addRow(page, 1, router.partitionHashOfRow(page, 1));
                assertThat("a real key must never land on the reserved null group", nonNullGroup, greaterThan(0));
            }

            // seenGroupIds/numKeys must now account for the null group, exactly as the normal
            // add(Page, AddInput) path would after seeing a null key.
            assertThat(hash.numKeys(), equalTo(2));
            try (IntVector selected = blockFactory.newIntArrayVector(new int[] { 0 }, 1)) {
                Block[] keysBack = hash.getKeys(selected);
                try {
                    assertThat(keysBack[0].isNull(0), equalTo(true));
                } finally {
                    Releasables.close(keysBack);
                }
            }
        }
    }

    public void testAddRowConsistentWithNormalAdd() {
        assumeTrue("SwissHash not available on this JVM", HashImplFactory.SWISS_HASH_AVAILABLE);
        // Insert some keys through the normal add(Page, AddInput) path first, then confirm the
        // router dedups against those existing group ids rather than assigning fresh ones -
        // both entry points share the same underlying hash table.
        try (BlockHash hash = longBlockHash()) {
            long[] seedValues = { 10, 20, 30 };
            Map<Long, Integer> viaAdd = addThroughNormalPath(hash, seedValues);

            BlockHash.Router router = hash.router();
            assertThat(router, notNullValue());
            try (LongBlock probe = blockFactory.newLongArrayVector(seedValues, seedValues.length).asBlock()) {
                Page page = new Page(probe);
                for (int i = 0; i < seedValues.length; i++) {
                    int partitionHash = router.partitionHashOfRow(page, i);
                    int groupId = router.addRow(page, i, partitionHash);
                    assertThat("router must see the same group id add() already assigned", groupId, equalTo(viaAdd.get(seedValues[i])));
                }
            }

            // And a brand-new key inserted through the router must, in turn, be visible to the
            // normal add() path as the same group id.
            long[] newValue = { 40 };
            try (LongBlock probe = blockFactory.newLongArrayVector(newValue, 1).asBlock()) {
                Page page = new Page(probe);
                int partitionHash = router.partitionHashOfRow(page, 0);
                int routedGroupId = router.addRow(page, 0, partitionHash);
                Map<Long, Integer> viaAddAfter = addThroughNormalPath(hash, newValue);
                assertThat(viaAddAfter.get(newValue[0]), equalTo(routedGroupId));
            }
        }
    }

    /**
     * Inserts {@code values} through {@link BlockHash#add} and returns the group id assigned to
     * each distinct value.
     */
    private Map<Long, Integer> addThroughNormalPath(BlockHash hash, long[] values) {
        Map<Long, Integer> result = new HashMap<>();
        try (LongBlock block = blockFactory.newLongArrayVector(values, values.length).asBlock()) {
            hash.add(new Page(block), new GroupingAggregatorFunction.AddInput() {
                private void record(IntBlock groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        result.put(values[p], groupIds.getInt(p));
                    }
                }

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    record(groupIds);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    record(groupIds);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        result.put(values[p], groupIds.getInt(p));
                    }
                }

                @Override
                public void close() {}
            });
        }
        return result;
    }
}
