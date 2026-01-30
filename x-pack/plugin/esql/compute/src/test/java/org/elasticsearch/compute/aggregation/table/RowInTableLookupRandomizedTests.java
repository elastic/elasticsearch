/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.blockhash.BlockHashRandomizedTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeTests;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.test.BlockTestUtils.append;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//@TestLogging(value = "org.elasticsearch.compute:TRACE", reason = "debug")
public class RowInTableLookupRandomizedTests extends ESTestCase {
    private static final int TRIES = 100;
    private static final int ROW_COUNT = 1000;

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (int keysPerPosition : new int[] { 1, 2 }) {
            for (int groups : new int[] { 1, 2, 5, 10 }) {
                params.add(
                    new Object[] {
                        groups,
                        MultivalueDedupeTests.supportedTypes(),
                        IntStream.range(0, groups).mapToObj(i -> RANDOM_KEY_ELEMENT).toList(),
                        keysPerPosition,
                        1000,
                        any(RowInTableLookup.class) }
                );
            }
            params.add(
                new Object[] {
                    1,
                    List.of(ElementType.INT),
                    List.of(ASCENDING),
                    keysPerPosition,
                    1000,
                    any(AscendingSequenceRowInTableLookup.class) }
            );
        }
        return params;
    }

    interface Generator {
        Object gen(ElementType elementType, int row);
    }

    private final int groups;
    private final List<ElementType> allowedTypes;
    private final List<Generator> generators;
    private final int keysPerPosition;
    private final int maxTableSize;
    private final Matcher<RowInTableLookup> expectedImplementation;

    public RowInTableLookupRandomizedTests(
        @Name("groups") int groups,
        @Name("allowedTypes") List<ElementType> allowedTypes,
        @Name("generator") List<Generator> generators,
        @Name("keysPerPosition") int keysPerPosition,
        @Name("maxTableSize") int maxTableSize,
        @Name("expectedImplementation") Matcher<RowInTableLookup> expectedImplementation

    ) {
        this.groups = groups;
        this.allowedTypes = allowedTypes;
        this.generators = generators;
        this.keysPerPosition = keysPerPosition;
        this.maxTableSize = maxTableSize;
        this.expectedImplementation = expectedImplementation;
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
        try (Table table = randomTable(blockFactory); RowInTableLookup offsetInTable = RowInTableLookup.build(blockFactory, table.blocks)) {
            assertThat(offsetInTable, expectedImplementation);
            for (int t = 0; t < TRIES; t++) {
                ByteSizeValue target = ByteSizeValue.ofKb(between(1, 100));
                try (
                    ToLookup toLookup = toLookup(blockFactory, table);
                    ReleasableIterator<IntBlock> actual = offsetInTable.lookup(toLookup.rows, target);
                ) {
                    int expectedIdx = 0;
                    while (actual.hasNext()) {
                        try (IntBlock lookedup = actual.next()) {
                            assertThat(lookedup.ramBytesUsed(), lessThan(target.getBytes() * 2));
                            if (keysPerPosition == 1) {
                                assertThat(lookedup.asVector(), not(nullValue()));
                            }
                            for (int p = 0; p < lookedup.getPositionCount(); p++) {
                                assertThat(lookedup.isNull(p), equalTo(false));
                                int start = lookedup.getFirstValueIndex(p);
                                int end = start + lookedup.getValueCount(p);
                                Set<Integer> actualRows = new TreeSet<>();
                                for (int i = start; i < end; i++) {
                                    actualRows.add(lookedup.getInt(i));
                                }
                                assertThat(actualRows, equalTo(toLookup.expected.get(expectedIdx)));
                                expectedIdx++;
                            }
                        }
                    }
                    assertThat(expectedIdx, equalTo(toLookup.expected.size()));
                }
            }
        }
    }

    private record Table(List<List<Object>> keys, Map<List<Object>, Integer> keyToRow, Block[] blocks) implements Releasable {
        @Override
        public void close() {
            Releasables.close(blocks);
        }
    }

    private Table randomTable(BlockFactory blockFactory) {
        List<List<Object>> keys = new ArrayList<>(maxTableSize);
        Map<List<Object>, Integer> keyToRow = new HashMap<>(maxTableSize);
        ElementType[] elementTypes = new ElementType[groups];
        Block.Builder[] builders = new Block.Builder[groups];
        try {
            for (int g = 0; g < groups; g++) {
                elementTypes[g] = randomFrom(allowedTypes);
                builders[g] = elementTypes[g].newBlockBuilder(maxTableSize, blockFactory);
            }
            for (int k = 0; k < maxTableSize; k++) {
                List<Object> key = new ArrayList<>(groups);
                for (int g = 0; g < groups; g++) {
                    key.add(generators.get(g).gen(elementTypes[g], k));
                }
                if (keyToRow.putIfAbsent(key, keys.size()) == null) {
                    /*
                     * Duplicate keys aren't allowed in constructors for OffsetInTable
                     * so just skip them. In most cases we'll have exactly maxTableSize
                     * entries, but sometimes, say if the generator is `boolean, boolean`
                     * we'll end up with fewer. That's fine.
                     */
                    keys.add(key);
                    for (int g = 0; g < groups; g++) {
                        append(builders[g], key.get(g));
                    }
                }
            }
            return new Table(keys, keyToRow, Block.Builder.buildAll(builders));
        } finally {
            Releasables.close(builders);
        }
    }

    private record ToLookup(Page rows, List<Set<Integer>> expected) implements Releasable {
        @Override
        public void close() {
            rows.releaseBlocks();
        }
    }

    ToLookup toLookup(BlockFactory blockFactory, Table table) {
        List<Set<Integer>> expected = new ArrayList<>(ROW_COUNT);
        Block.Builder[] builders = new Block.Builder[groups];
        try {
            for (int g = 0; g < groups; g++) {
                builders[g] = table.blocks[g].elementType().newBlockBuilder(ROW_COUNT, blockFactory);
            }
            for (int r = 0; r < ROW_COUNT; r++) {
                /*
                 * Pick some number of "generatorKeys" to be seed this position.
                 * We then populate this position with all the values for every column
                 * in this position. So if the seed values are `(1, a)`, `(2, b)`, and `(3, c)`
                 * then the values in the positions will be:
                 * <code>
                 *   n=[1, 2, 3], s=[a, b, c]
                 * </code>
                 *
                 * Lookup will combinatorially explode those into something like
                 * `(1, a)`, `(1, b)`, `(1, c)`, ... `(3, c)`. Which contains *at least*
                 * the seed keys. We calculate the expected value based on the combinatorial
                 * explosion.
                 *
                 * `null` in a key is funky because it means "no value" - so it doesn't
                 * participate in combinatorial explosions. We just don't add that value to
                 * the list. So the further combinatorial explosion *won't* contain the
                 * seed key that contained null. In fact, you can only match seed keys containing
                 * null if all values are null. That only happens if all the values for
                 * that column are null. That's certainly possible with `null` typed columns
                 * or if you get very lucky.
                 */
                List<List<Object>> generatorKeys = IntStream.range(0, keysPerPosition)
                    .mapToObj(k -> table.keys.get(between(0, table.keys.size() - 1)))
                    .toList();
                for (int g = 0; g < groups; g++) {
                    List<Object> values = new ArrayList<>(generatorKeys.size());
                    for (List<Object> key : generatorKeys) {
                        Object v = key.get(g);
                        if (v != null) {
                            values.add(v);
                        }
                    }
                    append(builders[g], values);
                }
                List<List<Object>> explosion = combinatorialExplosion(generatorKeys);
                for (List<Object> generatorKey : generatorKeys) {
                    /*
                     * All keys should be in the explosion of values. Except keys
                     * containing `null`. *Except except* if those keys are the
                     * only column. In that case there really aren't any values
                     * for this column - so null "shines through".
                     */
                    if (explosion.size() == 1 || generatorKey.stream().noneMatch(Objects::isNull)) {
                        assertThat(explosion, hasItem(generatorKey));
                    }
                }
                Set<Integer> expectedAtPosition = new TreeSet<>();
                for (List<Object> v : explosion) {
                    Integer row = table.keyToRow.get(v);
                    if (row != null) {
                        expectedAtPosition.add(row);
                    }
                }
                expected.add(expectedAtPosition);
            }
            return new ToLookup(new Page(Block.Builder.buildAll(builders)), expected);
        } finally {
            Releasables.close(builders);
        }
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }

    private static final Generator RANDOM_KEY_ELEMENT = new Generator() {
        @Override
        public Object gen(ElementType elementType, int row) {
            return BlockHashRandomizedTests.randomKeyElement(elementType);
        }

        @Override
        public String toString() {
            return "randomKeyElement";
        }
    };

    private static final Generator ASCENDING = new Generator() {
        @Override
        public Object gen(ElementType elementType, int row) {
            return switch (elementType) {
                case INT -> row;
                case LONG -> (long) row;
                case DOUBLE -> (double) row;
                default -> throw new IllegalArgumentException("bad element type [" + elementType + "]");
            };
        }

        @Override
        public String toString() {
            return "ascending";
        }
    };

    private List<List<Object>> combinatorialExplosion(List<List<Object>> values) {
        List<Set<Object>> uniqueValues = IntStream.range(0, groups).mapToObj(i -> (Set<Object>) new HashSet<>()).toList();
        for (List<Object> v : values) {
            for (int g = 0; g < groups; g++) {
                uniqueValues.get(g).add(v.get(g));
            }
        }
        return combinatorialExplosion(List.of(List.of()), uniqueValues);
    }

    private List<List<Object>> combinatorialExplosion(List<List<Object>> soFar, List<Set<Object>> remaining) {
        if (remaining.isEmpty()) {
            return soFar;
        }
        List<List<Object>> result = new ArrayList<>();
        for (List<Object> start : soFar) {
            for (Object v : remaining.get(0)) {
                List<Object> values = new ArrayList<>(start.size() + 1);
                values.addAll(start);
                values.add(v);
                result.add(values);
            }
        }
        return combinatorialExplosion(result, remaining.subList(1, remaining.size()));
    }
}
