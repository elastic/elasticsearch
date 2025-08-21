/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.data.BasicBlockTests.assertInsertNulls;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class BlockMultiValuedTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType e : ElementType.values()) {
            if (e == ElementType.UNKNOWN
                || e == ElementType.NULL
                || e == ElementType.DOC
                || e == ElementType.COMPOSITE
                || e == ElementType.AGGREGATE_METRIC_DOUBLE) {
                continue;
            }
            for (boolean nullAllowed : new boolean[] { false, true }) {
                params.add(new Object[] { e, nullAllowed });
            }
        }
        return params;
    }

    private final ElementType elementType;
    private final boolean nullAllowed;

    public BlockMultiValuedTests(@Name("elementType") ElementType elementType, @Name("nullAllowed") boolean nullAllowed) {
        this.elementType = elementType;
        this.nullAllowed = nullAllowed;
    }

    public void testMultiValued() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        try {
            assertThat(b.block().getPositionCount(), equalTo(positionCount));
            assertThat(b.block().getTotalValueCount(), equalTo(b.valueCount()));
            for (int p = 0; p < positionCount; p++) {
                BlockTestUtils.assertPositionValues(b.block(), p, equalTo(b.values().get(p)));
            }

            assertThat(b.block().mayHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));
            assertThat(b.block().doesHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));
            assertInsertNulls(b.block());
        } finally {
            b.block().close();
        }
    }

    public void testExpand() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 100, 0, 0);
        assertExpanded(b.block());
    }

    public void testFilteredNoop() {
        assertFiltered(true, false);
    }

    public void testFilteredReordered() {
        assertFiltered(true, true);
    }

    public void testFilteredSubset() {
        assertFiltered(false, false);
    }

    public void testFilteredJumbledSubset() {
        assertFiltered(false, true);
    }

    public void testFilteredNoopThenExpanded() {
        assertFilteredThenExpanded(true, false);
    }

    public void testFilteredReorderedThenExpanded() {
        assertFilteredThenExpanded(true, true);
    }

    public void testFilteredSubsetThenExpanded() {
        assertFilteredThenExpanded(false, false);
    }

    public void testFilteredJumbledSubsetThenExpanded() {
        assertFilteredThenExpanded(false, true);
    }

    public void testLookupFromSingleOnePage() {
        assertLookup(ByteSizeValue.ofMb(100), between(1, 32), p -> 1);
    }

    public void testLookupFromManyOnePage() {
        assertLookup(ByteSizeValue.ofMb(100), between(1, 32), p -> between(1, 5));
    }

    public void testLookupFromSingleManyPages() {
        assertLookup(ByteSizeValue.ofBytes(1), between(1, 32), p -> 1);
    }

    public void testToMask() {
        if (elementType != ElementType.BOOLEAN) {
            return;
        }
        int positionCount = randomIntBetween(0, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 2, 10, 0, 0);
        try (ToMask mask = ((BooleanBlock) b.block()).toMask()) {
            var anyNonNullValue = b.values().stream().anyMatch(Objects::nonNull);
            assertThat(mask.hadMultivaluedFields(), equalTo(anyNonNullValue));

            for (int p = 0; p < b.values().size(); p++) {
                List<Object> v = b.values().get(p);
                if (v == null) {
                    assertThat(mask.mask().getBoolean(p), equalTo(false));
                    continue;
                }
                if (v.size() != 1) {
                    assertThat(mask.mask().getBoolean(p), equalTo(false));
                    continue;
                }
                assertThat(mask.mask().getBoolean(p), equalTo(v.get(0)));
            }
        } finally {
            b.block().close();
        }
    }

    public void testMask() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        try (
            BooleanVector mask = BasicBlockTests.randomMask(b.values().size() + between(0, 1000));
            Block masked = b.block().keepMask(mask)
        ) {
            for (int p = 0; p < b.values().size(); p++) {
                List<Object> inputValues = b.values().get(p);
                List<Object> valuesAtPosition = valuesAtPositions(masked, p, p + 1).get(0);
                if (inputValues == null || mask.getBoolean(p) == false) {
                    assertThat(masked.isNull(p), equalTo(true));
                    assertThat(valuesAtPosition, nullValue());
                    continue;
                }
                assertThat(masked.isNull(p), equalTo(false));
                assertThat(valuesAtPosition, equalTo(inputValues));
            }
        } finally {
            b.block().close();
        }
    }

    public void testInsertNull() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 2, 10, 0, 0);
        try {
            assertInsertNulls(b.block());
        } finally {
            b.block().close();
        }
    }

    private void assertFiltered(boolean all, boolean shuffled) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        try {
            int[] positions = randomFilterPositions(b.block(), all, shuffled);
            Block filtered = b.block().filter(positions);
            try {
                assertThat(filtered.getPositionCount(), equalTo(positions.length));

                int expectedValueCount = 0;
                for (int p : positions) {
                    List<Object> values = b.values().get(p);
                    if (values != null) {
                        expectedValueCount += values.size();
                    }
                }
                assertThat(filtered.getTotalValueCount(), equalTo(expectedValueCount));
                for (int r = 0; r < positions.length; r++) {
                    if (b.values().get(positions[r]) == null) {
                        assertThat(filtered.getValueCount(r), equalTo(0));
                        assertThat(filtered.isNull(r), equalTo(true));
                    } else {
                        assertThat(filtered.getValueCount(r), equalTo(b.values().get(positions[r]).size()));
                        assertThat(valuesAtPositions(filtered, r, r + 1).get(0), equalTo(b.values().get(positions[r])));
                    }
                }
            } finally {
                filtered.close();
            }
            assertThat(b.block().mayHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));
            assertThat(b.block().doesHaveMultivaluedFields(), equalTo(b.values().stream().anyMatch(l -> l != null && l.size() > 1)));

        } finally {
            b.block().close();
        }
    }

    private int[] randomFilterPositions(Block orig, boolean all, boolean shuffled) {
        int[] positions = IntStream.range(0, orig.getPositionCount()).toArray();
        if (shuffled) {
            Randomness.shuffle(Arrays.asList(positions));
        }
        if (all) {
            return positions;
        }
        return IntStream.range(0, between(1, orig.getPositionCount())).map(i -> positions[i]).toArray();
    }

    private void assertExpanded(Block orig) {
        try (orig; Block expanded = orig.expand()) {
            assertThat(expanded.getTotalValueCount(), equalTo(orig.getTotalValueCount()));

            int np = 0;
            for (int op = 0; op < orig.getPositionCount(); op++) {
                if (orig.isNull(op)) {
                    assertThat(expanded.isNull(np), equalTo(true));
                    assertThat(expanded.getValueCount(np++), equalTo(0));
                    continue;
                }
                List<Object> oValues = valuesAtPositions(orig, op, op + 1).get(0);
                for (Object ov : oValues) {
                    assertThat(expanded.isNull(np), equalTo(false));
                    assertThat(expanded.getValueCount(np), equalTo(1));
                    assertThat(valuesAtPositions(expanded, np, ++np).get(0), equalTo(List.of(ov)));
                }
            }
        }
    }

    private void assertFilteredThenExpanded(boolean all, boolean shuffled) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 10, 0, 0);
        try {
            int[] positions = randomFilterPositions(b.block(), all, shuffled);
            assertExpanded(b.block().filter(positions));
        } finally {
            b.block().close();
        }
    }

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    /**
     * A {@link DriverContext} with a breaking {@link BigArrays} and {@link BlockFactory}.
     */
    protected BlockFactory blockFactory() { // TODO move this to driverContext once everyone supports breaking
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return factory;
    }

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            for (var factory : blockFactories) {
                if (factory instanceof MockBlockFactory mockBlockFactory) {
                    mockBlockFactory.ensureAllBlocksAreReleased();
                }
            }
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }

    private void assertLookup(ByteSizeValue targetBytes, int positionsToCopy, IntUnaryOperator positionsPerPosition) {
        BlockFactory positionsFactory = blockFactory();
        int positionCount = randomIntBetween(100, 16 * 1024);
        var b = RandomBlock.randomBlock(blockFactory(), elementType, positionCount, nullAllowed, 0, 100, 0, 0);
        try (IntBlock.Builder builder = positionsFactory.newIntBlockBuilder(positionsToCopy);) {
            for (int p = 0; p < positionsToCopy; p++) {
                int max = positionsPerPosition.applyAsInt(p);
                switch (max) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendInt(between(0, positionCount + 100));
                    default -> {
                        builder.beginPositionEntry();
                        for (int v = 0; v < max; v++) {
                            builder.appendInt(between(0, positionCount + 100));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            Block copy = null;
            int positionOffset = 0;
            try (
                IntBlock positions = builder.build();
                ReleasableIterator<? extends Block> lookup = b.block().lookup(positions, targetBytes);
            ) {
                for (int p = 0; p < positions.getPositionCount(); p++) {
                    if (copy == null || p - positionOffset == copy.getPositionCount()) {
                        if (copy != null) {
                            positionOffset += copy.getPositionCount();
                            copy.close();
                        }
                        assertThat(lookup.hasNext(), equalTo(true));
                        copy = lookup.next();
                        if (positions.getPositionCount() - positionOffset < Operator.MIN_TARGET_PAGE_SIZE) {
                            assertThat(copy.getPositionCount(), equalTo(positions.getPositionCount() - positionOffset));
                        } else {
                            assertThat(copy.getPositionCount(), greaterThanOrEqualTo(Operator.MIN_TARGET_PAGE_SIZE));
                        }
                    }
                    List<Object> expected = new ArrayList<>();
                    int start = positions.getFirstValueIndex(p);
                    int end = start + positions.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        int toCopy = positions.getInt(i);
                        if (toCopy < b.block().getPositionCount()) {
                            List<Object> v = valuesAtPositions(b.block(), toCopy, toCopy + 1).get(0);
                            if (v != null) {
                                expected.addAll(v);
                            }
                        }
                    }
                    if (expected.isEmpty()) {
                        assertThat(copy.isNull(p - positionOffset), equalTo(true));
                    } else {
                        assertThat(copy.isNull(p - positionOffset), equalTo(false));
                        assertThat(valuesAtPositions(copy, p - positionOffset, p + 1 - positionOffset).get(0), equalTo(expected));
                    }
                }
                assertThat(lookup.hasNext(), equalTo(false));
            } finally {
                Releasables.close(copy);
            }
        } finally {
            b.block().close();
        }
    }
}
