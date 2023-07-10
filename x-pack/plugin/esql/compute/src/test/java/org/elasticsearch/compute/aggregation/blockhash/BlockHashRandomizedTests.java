/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.MultivalueDedupeTests;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

//@TestLogging(value = "org.elasticsearch.compute:TRACE", reason = "debug")
public class BlockHashRandomizedTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (boolean forcePackedHash : new boolean[] { false, true }) {
            for (int groups : new int[] { 1, 2, 3, 4, 5, 10 }) {
                for (int maxValuesPerPosition : new int[] { 1, 3 }) {
                    for (int dups : new int[] { 0, 2 }) {
                        for (List<ElementType> allowedTypes : List.of(
                            /*
                             * Run with only `LONG` elements because we have some
                             * optimizations that hit if you only have those.
                             */
                            List.of(ElementType.LONG),
                            /*
                             * Run with only `LONG` and `BYTES_REF` elements because
                             * we have some optimizations that hit if you only have
                             * those.
                             */
                            List.of(ElementType.LONG, ElementType.BYTES_REF),
                            MultivalueDedupeTests.supportedTypes()
                        )) {
                            params.add(new Object[] { forcePackedHash, groups, maxValuesPerPosition, dups, allowedTypes });
                        }
                    }
                }
            }
        }
        return params;
    }

    private final boolean forcePackedHash;
    private final int groups;
    private final int maxValuesPerPosition;
    private final int dups;
    private final List<ElementType> allowedTypes;

    public BlockHashRandomizedTests(
        boolean forcePackedHash,
        int groups,
        int maxValuesPerPosition,
        int dups,
        List<ElementType> allowedTypes
    ) {
        this.forcePackedHash = forcePackedHash;
        this.groups = groups;
        this.maxValuesPerPosition = maxValuesPerPosition;
        this.dups = dups;
        this.allowedTypes = allowedTypes;
    }

    public void test() {
        List<ElementType> types = randomList(groups, groups, () -> randomFrom(allowedTypes));
        BasicBlockTests.RandomBlock[] randomBlocks = new BasicBlockTests.RandomBlock[types.size()];
        Block[] blocks = new Block[types.size()];
        int pageCount = between(1, 10);
        int positionCount = 100;
        int emitBatchSize = 100;
        try (BlockHash blockHash = newBlockHash(emitBatchSize, types)) {
            Oracle oracle = new Oracle();

            for (int p = 0; p < pageCount; p++) {
                for (int g = 0; g < blocks.length; g++) {
                    randomBlocks[g] = BasicBlockTests.randomBlock(
                        types.get(g),
                        positionCount,
                        randomBoolean(),
                        1,
                        maxValuesPerPosition,
                        0,
                        dups
                    );
                    blocks[g] = randomBlocks[g].block();
                }
                oracle.add(randomBlocks);
                int[] batchCount = new int[1];
                BlockHashTests.hash(blockHash, ordsAndKeys -> {
                    if (forcePackedHash == false) {
                        if (types.equals(List.of(ElementType.LONG, ElementType.LONG))) {
                            // For now we only have defense against big blocks in the long/long hash
                            assertThat(ordsAndKeys.ords().getTotalValueCount(), lessThanOrEqualTo(emitBatchSize));
                        }
                    }
                    batchCount[0]++;
                }, blocks);
                if (types.size() == 1) {
                    assertThat(batchCount[0], equalTo(1));
                }
            }

            Block[] keyBlocks = blockHash.getKeys();
            Set<List<Object>> keys = new TreeSet<>(new KeyComparator());
            for (int p = 0; p < keyBlocks[0].getPositionCount(); p++) {
                List<Object> key = new ArrayList<>(keyBlocks.length);
                for (Block keyBlock : keyBlocks) {
                    if (keyBlock.isNull(p)) {
                        key.add(null);
                    } else {
                        key.add(BasicBlockTests.valuesAtPositions(keyBlock, p, p + 1).get(0).get(0));
                        assertThat(keyBlock.getValueCount(p), equalTo(1));
                    }
                }
                boolean contained = keys.add(key);
                assertTrue(contained);
            }

            assertThat(keys, equalTo(oracle.keys));
        }
    }

    private BlockHash newBlockHash(int emitBatchSize, List<ElementType> types) {
        List<HashAggregationOperator.GroupSpec> specs = new ArrayList<>(types.size());
        for (int c = 0; c < types.size(); c++) {
            specs.add(new HashAggregationOperator.GroupSpec(c, types.get(c)));
        }
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());
        return forcePackedHash ? new PackedValuesBlockHash(specs, bigArrays) : BlockHash.build(specs, bigArrays, emitBatchSize);
    }

    private static class KeyComparator implements Comparator<List<?>> {
        @Override
        public int compare(List<?> lhs, List<?> rhs) {
            for (int i = 0; i < lhs.size(); i++) {
                @SuppressWarnings("unchecked")
                Comparable<Object> l = (Comparable<Object>) lhs.get(i);
                Object r = rhs.get(i);
                if (l == null) {
                    if (r == null) {
                        continue;
                    } else {
                        return 1;
                    }
                }
                if (r == null) {
                    return -1;
                }
                int cmp = l.compareTo(r);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }

    private static class Oracle {
        private final NavigableSet<List<Object>> keys = new TreeSet<>(new KeyComparator());

        void add(BasicBlockTests.RandomBlock[] randomBlocks) {
            for (int p = 0; p < randomBlocks[0].block().getPositionCount(); p++) {
                add(randomBlocks, p, List.of());
            }
        }

        void add(BasicBlockTests.RandomBlock[] randomBlocks, int p, List<Object> key) {
            if (key.size() == randomBlocks.length) {
                keys.add(key);
                return;
            }
            BasicBlockTests.RandomBlock block = randomBlocks[key.size()];
            List<Object> values = block.values().get(p);
            if (values == null) {
                return;
            }
            for (Object v : values) {
                List<Object> newKey = new ArrayList<>(key);
                newKey.add(v);
                add(randomBlocks, p, newKey);
            }
        }
    }
}
