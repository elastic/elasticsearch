/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.MockBlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.MultivalueDedupeTests;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ListMatcher;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        @Name("forcePackedHash") boolean forcePackedHash,
        @Name("groups") int groups,
        @Name("maxValuesPerPosition") int maxValuesPerPosition,
        @Name("dups") int dups,
        @Name("allowedTypes") List<ElementType> allowedTypes
    ) {
        this.forcePackedHash = forcePackedHash;
        this.groups = groups;
        this.maxValuesPerPosition = maxValuesPerPosition;
        this.dups = dups;
        this.allowedTypes = allowedTypes;
    }

    public void test() {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
        test(new MockBlockFactory(breaker, bigArrays));
    }

    public void testWithCranky() {
        CircuitBreakerService service = new CrankyCircuitBreakerService();
        CircuitBreaker breaker = service.getBreaker(CircuitBreaker.REQUEST);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, service);
        try {
            test(new MockBlockFactory(breaker, bigArrays));
            logger.info("cranky let us finish!");
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void test(MockBlockFactory blockFactory) {
        List<ElementType> types = randomList(groups, groups, () -> randomFrom(allowedTypes));
        BasicBlockTests.RandomBlock[] randomBlocks = new BasicBlockTests.RandomBlock[types.size()];
        Block[] blocks = new Block[types.size()];
        int pageCount = between(1, 10);
        int positionCount = 100;
        int emitBatchSize = 100;
        try (BlockHash blockHash = newBlockHash(blockFactory, emitBatchSize, types)) {
            /*
             * Only the long/long, long/bytes_ref, and bytes_ref/long implementations don't collect nulls.
             */
            Oracle oracle = new Oracle(
                forcePackedHash
                    || false == (types.equals(List.of(ElementType.LONG, ElementType.LONG))
                        || types.equals(List.of(ElementType.LONG, ElementType.BYTES_REF))
                        || types.equals(List.of(ElementType.BYTES_REF, ElementType.LONG)))
            );

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
                // PackedValuesBlockHash always chunks but the normal single value ones don't
                boolean usingSingle = forcePackedHash == false && types.size() == 1;
                BlockHashTests.hash(false, blockHash, ordsAndKeys -> {
                    if (usingSingle == false) {
                        assertThat(ordsAndKeys.ords().getTotalValueCount(), lessThanOrEqualTo(emitBatchSize));
                    }
                    batchCount[0]++;
                }, blocks);
                if (usingSingle) {
                    assertThat(batchCount[0], equalTo(1));
                }
            }

            Block[] keyBlocks = blockHash.getKeys();
            try {
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

                if (false == keys.equals(oracle.keys)) {
                    List<List<Object>> keyList = new ArrayList<>();
                    keyList.addAll(keys);
                    ListMatcher keyMatcher = matchesList();
                    for (List<Object> k : oracle.keys) {
                        keyMatcher = keyMatcher.item(k);
                    }
                    assertMap(keyList, keyMatcher);
                }
            } finally {
                Releasables.closeExpectNoException(keyBlocks);
                blockFactory.ensureAllBlocksAreReleased();
            }
        }
        assertThat(blockFactory.breaker().getUsed(), is(0L));
    }

    private BlockHash newBlockHash(BlockFactory blockFactory, int emitBatchSize, List<ElementType> types) {
        List<HashAggregationOperator.GroupSpec> specs = new ArrayList<>(types.size());
        for (int c = 0; c < types.size(); c++) {
            specs.add(new HashAggregationOperator.GroupSpec(c, types.get(c)));
        }
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        return forcePackedHash
            ? new PackedValuesBlockHash(specs, driverContext, emitBatchSize)
            : BlockHash.build(specs, driverContext, emitBatchSize, true);
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

        private final boolean collectsNull;

        private Oracle(boolean collectsNull) {
            this.collectsNull = collectsNull;
        }

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
                if (collectsNull) {
                    List<Object> newKey = new ArrayList<>(key);
                    newKey.add(null);
                    add(randomBlocks, p, newKey);
                }
                return;
            }
            for (Object v : values) {
                List<Object> newKey = new ArrayList<>(key);
                newKey.add(v);
                add(randomBlocks, p, newKey);
            }
        }
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }
}
