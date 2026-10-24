/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PartitionedRowInTableLookupBuilderTests extends ESTestCase {
    private static final List<List<ElementType>> KEY_TYPES = List.of(
        List.of(ElementType.INT),
        List.of(ElementType.LONG),
        List.of(ElementType.BYTES_REF),
        List.of(ElementType.LONG, ElementType.INT),
        List.of(ElementType.INT, ElementType.LONG),
        List.of(ElementType.LONG, ElementType.BYTES_REF),
        List.of(ElementType.BYTES_REF, ElementType.LONG),
        List.of(ElementType.BYTES_REF, ElementType.BYTES_REF),
        List.of(ElementType.BYTES_REF, ElementType.BYTES_REF, ElementType.BYTES_REF),
        List.of(ElementType.INT, ElementType.BYTES_REF),
        List.of(ElementType.LONG, ElementType.BOOLEAN),
        List.of(ElementType.DOUBLE, ElementType.LONG)
    );

    private final BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();

    @Before
    public void requirePartitioning() {
        assumeTrue("partitioning requires the swiss hash", PartitionedBlockHash.supportPartitioning());
    }

    /**
     * The builder needs every hash {@link PartitionedBlockHash#buildForLookup} picks for keys that
     * {@link PartitionedBlockHash#supportsPartitionedLookup} to be a {@link PartitionedBlockHash}, and the operator
     * only falls back to a serial build for keys that don't.
     */
    public void testSupportsPartitionedLookupMatchesBuiltHash() {
        List<ElementType> types = List.of(
            ElementType.NULL,
            ElementType.BOOLEAN,
            ElementType.INT,
            ElementType.LONG,
            ElementType.DOUBLE,
            ElementType.DOUBLE_RANGE,
            ElementType.BYTES_REF
        );
        List<List<ElementType>> shapes = new ArrayList<>();
        for (ElementType first : types) {
            shapes.add(List.of(first));
            for (ElementType second : types) {
                shapes.add(List.of(first, second));
            }
        }
        shapes.add(List.of(ElementType.BYTES_REF, ElementType.BYTES_REF, ElementType.BYTES_REF));
        shapes.add(List.of(ElementType.INT, ElementType.LONG, ElementType.BYTES_REF));
        for (List<ElementType> shape : shapes) {
            List<BlockHash.GroupSpec> groups = new ArrayList<>(shape.size());
            for (int k = 0; k < shape.size(); k++) {
                groups.add(new BlockHash.GroupSpec(k, shape.get(k)));
            }
            try (BlockHash hash = PartitionedBlockHash.buildForLookup(groups, nonBreaking)) {
                assertThat(
                    shape.toString(),
                    PartitionedBlockHash.supportsPartitionedLookup(groups),
                    equalTo(hash instanceof PartitionedBlockHash)
                );
            }
        }
    }

    public void testRejectsKeysWithoutPartitionedLookup() {
        try (
            Block key = randomBoolean() ? nonBreaking.newConstantDoubleBlockWith(1.0, 1) : nonBreaking.newConstantNullBlock(between(1, 100))
        ) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new PartitionedRowInTableLookupBuilder(nonBreaking, new Block[] { key }, ActionListener.noop())
            );
        }
    }

    /**
     * Built by several threads from slices of random size, the table must answer every probe exactly like the
     * table one thread builds, including the order of each key's rows.
     */
    public void testMatchesSerialBuild() throws Exception {
        List<ElementType> types = randomFrom(KEY_TYPES);
        List<List<Object>> pools = pools(types, between(1, 500));
        Block[] keys = buildKeys(types, pools, between(1, 5_000));
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        MockBlockFactory blockFactory = new MockBlockFactory(
            BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
        );
        try (
            RowInTableLookup serial = RowInTableLookup.build(nonBreaking, keys, true);
            RowInTableLookup partitioned = buildPartitioned(blockFactory, keys, between(1, 2_000), between(1, 4))
        ) {
            for (int t = 0; t < 20; t++) {
                Page probe = probe(types, pools, between(1, 1_000));
                try {
                    assertThat(lookup(partitioned, probe), equalTo(lookup(serial, probe)));
                } finally {
                    probe.releaseBlocks();
                }
            }
        } finally {
            Releasables.close(keys);
        }
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    /**
     * A build that trips the breaker fails every caller's listener and releases everything once closed.
     */
    public void testBreakerFailureReleasesEverything() throws Exception {
        List<ElementType> types = randomFrom(KEY_TYPES);
        List<List<Object>> pools = pools(types, between(1, 500));
        Block[] keys = buildKeys(types, pools, between(1_000, 5_000));
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofBytes(between(1, 100_000)));
        MockBlockFactory blockFactory = new MockBlockFactory(
            BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
        );
        try {
            PlainActionFuture<RowInTableLookup> future = new PlainActionFuture<>();
            try (var builder = new PartitionedRowInTableLookupBuilder(blockFactory, keys, between(100, 1_000), future)) {
                startInParallel(between(1, 4), t -> {
                    try {
                        builder.help();
                    } catch (CircuitBreakingException e) {
                        // the failure reaches the listener
                    }
                });
                assertTrue(future.isDone());
                assertTrue(builder.slicesDone().isDone());
                try {
                    future.get().close();
                } catch (ExecutionException e) {
                    assertThat(e.getCause(), instanceOf(CircuitBreakingException.class));
                }
            }
        } finally {
            Releasables.close(keys);
        }
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private RowInTableLookup buildPartitioned(BlockFactory blockFactory, Block[] keys, int sliceRows, int threads) throws Exception {
        PlainActionFuture<RowInTableLookup> future = new PlainActionFuture<>();
        try (var builder = new PartitionedRowInTableLookupBuilder(blockFactory, keys, sliceRows, future)) {
            startInParallel(threads, t -> builder.help());
            assertTrue(future.isDone());
            return future.get();
        }
    }

    private List<List<Integer>> lookup(RowInTableLookup table, Page probe) {
        List<List<Integer>> result = new ArrayList<>();
        try (ReleasableIterator<IntBlock> rows = table.lookup(probe, ByteSizeValue.ofKb(between(1, 100)))) {
            while (rows.hasNext()) {
                try (IntBlock block = rows.next()) {
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        List<Integer> positionRows = new ArrayList<>();
                        for (int i = 0; i < block.getValueCount(p); i++) {
                            positionRows.add(block.getInt(block.getFirstValueIndex(p) + i));
                        }
                        result.add(positionRows);
                    }
                }
            }
        }
        return result;
    }

    private List<List<Object>> pools(List<ElementType> types, int size) {
        List<List<Object>> pools = new ArrayList<>(types.size());
        for (ElementType type : types) {
            Set<Object> pool = new HashSet<>();
            int distinct = type == ElementType.BOOLEAN ? Math.min(size, 2) : size;
            while (pool.size() < distinct) {
                pool.add(randomValue(type));
            }
            pools.add(new ArrayList<>(pool));
        }
        return pools;
    }

    private Block[] buildKeys(List<ElementType> types, List<List<Object>> pools, int rows) {
        Block[] keys = new Block[types.size()];
        for (int k = 0; k < types.size(); k++) {
            try (Block.Builder builder = types.get(k).newBlockBuilder(rows, nonBreaking)) {
                for (int r = 0; r < rows; r++) {
                    BlockTestUtils.append(builder, randomFrom(pools.get(k)));
                }
                keys[k] = builder.build();
            }
        }
        return keys;
    }

    /**
     * Mostly keys the table has, plus keys it likely doesn't, nulls and multivalued keys.
     */
    private Page probe(List<ElementType> types, List<List<Object>> pools, int positions) {
        Block[] blocks = new Block[types.size()];
        for (int k = 0; k < types.size(); k++) {
            try (Block.Builder builder = types.get(k).newBlockBuilder(positions, nonBreaking)) {
                for (int p = 0; p < positions; p++) {
                    switch (between(0, 9)) {
                        case 0 -> builder.appendNull();
                        case 1 -> BlockTestUtils.append(builder, randomValue(types.get(k)));
                        case 2 -> BlockTestUtils.append(builder, List.of(randomFrom(pools.get(k)), randomValue(types.get(k))));
                        default -> BlockTestUtils.append(builder, randomFrom(pools.get(k)));
                    }
                }
                blocks[k] = builder.build();
            }
        }
        return new Page(blocks);
    }

    private static Object randomValue(ElementType type) {
        return switch (type) {
            case INT -> randomInt();
            case LONG -> randomLong();
            case DOUBLE -> randomDouble();
            case BOOLEAN -> randomBoolean();
            case BYTES_REF -> new BytesRef(randomAlphaOfLength(between(0, 10)));
            default -> throw new IllegalArgumentException("unsupported key type [" + type + "]");
        };
    }
}
