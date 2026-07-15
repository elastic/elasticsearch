/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Base tests for {@link Block} implementations built through their normal builders.
 */
public abstract class BlockTestCase<B extends Block, BB extends Block.Builder, V> extends ESTestCase {
    private final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
    private final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    protected abstract BB createBuilder(BlockFactory blockFactory, int estimatedSize);

    protected abstract void appendNull(BB builder);

    protected abstract void appendSingle(BB builder, V value);

    protected abstract void appendMultivalued(BB builder, List<V> values);

    protected abstract B build(BB builder);

    protected abstract List<V> valuesAt(B block, int position);

    protected abstract V randomValue();

    protected boolean supportsLookup() {
        return true;
    }

    protected boolean supportsDenseVector() {
        return true;
    }

    protected boolean supportsConstantBlockFactory() {
        return false;
    }

    protected B createConstantBlock(BlockFactory blockFactory, V value, int positions) {
        throw new UnsupportedOperationException();
    }

    protected void assertSingleValueBlockRepresentation(B block) {}

    protected void assertDenseVectorBlockRepresentation(B block) {}

    protected void assertArrayBlockRepresentation(B block) {}

    protected void assertBigArrayVectorBlockRepresentation(B block) {}

    protected void assertBigArrayBlockRepresentation(B block) {}

    protected void assertEmptyBlockRepresentation(B block) {}

    protected void assertAllNullBlockRepresentation(B block) {}

    protected void assertConstantBlockFactoryRepresentation(B block) {}

    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {}

    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {}

    protected void assertAdditionalInvariants(B block, List<List<V>> expected) {}

    public final void testSingleValueBlock() {
        List<List<V>> expected = List.of(List.of(randomValue()));
        try (B block = buildBlock(blockFactory(), expected)) {
            assertSingleValueBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testDenseVectorBlock() {
        List<List<V>> expected = List.of(List.of(randomValue()), List.of(randomValue()), List.of(randomValue()));
        try (B block = buildBlock(blockFactory(), expected)) {
            assertDenseVectorBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testBuilderGrowth() {
        for (int estimatedSize : List.of(0, 1, 2, 3, 4, 5)) {
            List<List<V>> expected = denseExpectedValues(10);
            try (B block = buildBlock(blockFactory(), estimatedSize, expected)) {
                assertDenseVectorBlockRepresentation(block);
                assertBlock(block, expected);
            }
        }
    }

    public final void testRandomDenseSingleValuedBlock() {
        List<List<V>> expected = denseExpectedValues(randomIntBetween(1, 16 * 1024));
        try (B block = buildBlock(blockFactory(), randomIntBetween(0, expected.size()), expected)) {
            assertBlock(block, expected);
        }
    }

    public final void testRandomSparseSingleValuedBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        List<List<V>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(randomBoolean() ? null : List.of(randomValue()));
        }
        try (B block = buildBlock(blockFactory(), randomIntBetween(0, positionCount), expected)) {
            assertBlock(block, expected);
        }
    }

    public final void testConstantBlockFactory() {
        assumeTrue("constant block factory unsupported", supportsConstantBlockFactory());
        V value = randomValue();
        int positions = randomIntBetween(5, 1024);
        try (B block = createConstantBlock(blockFactory(), value, positions)) {
            assertConstantBlockFactoryRepresentation(block);
            assertBlock(block, repeat(value, positions));
            assertConstantLookupBlockRepresentations(block, value);
        }
    }

    public final void testZeroPositionConstantBlockFactory() {
        assumeTrue("constant block factory unsupported", supportsConstantBlockFactory());
        try (B block = createConstantBlock(blockFactory(), randomValue(), 0)) {
            assertConstantBlockFactoryRepresentation(block);
            assertBlock(block, List.of());
        }
    }

    public final void testSinglePositionConstantBlockFactory() {
        assumeTrue("constant block factory unsupported", supportsConstantBlockFactory());
        V value = randomValue();
        try (B block = createConstantBlock(blockFactory(), value, 1)) {
            assertConstantBlockFactoryRepresentation(block);
            assertBlock(block, List.of(List.of(value)));
        }
    }

    private void assertConstantLookupBlockRepresentations(B block, V value) {
        if (supportsLookup() == false) {
            return;
        }
        try (IntBlock positions = positions(block.blockFactory(), 1, 3, 4)) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(true));
                try (Block lookedUp = lookup.next()) {
                    assertValues(castBlock(lookedUp), List.of(List.of(value), List.of(value), List.of(value)));
                    assertConstantInRangeLookupBlockRepresentation(lookedUp);
                }
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
        try (IntBlock positions = positions(block.blockFactory(), 1, 3, 4, new int[] { 1, 3, 4 })) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(true));
                try (Block lookedUp = lookup.next()) {
                    assertValues(
                        castBlock(lookedUp),
                        List.of(List.of(value), List.of(value), List.of(value), List.of(value, value, value))
                    );
                }
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
        try (IntBlock positions = positions(block.blockFactory(), block.getPositionCount() + 1000)) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(true));
                try (Block lookedUp = lookup.next()) {
                    List<List<V>> expected = new ArrayList<>();
                    expected.add(null);
                    assertValues(castBlock(lookedUp), expected);
                    assertConstantOutOfRangeLookupBlockRepresentation(lookedUp);
                }
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
    }

    public final void testArrayBlock() {
        List<List<V>> expected = mixedExpectedValues();
        try (B block = buildBlock(blockFactory(), expected)) {
            assertArrayBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testBigArrayVectorBlock() {
        List<List<V>> expected = List.of(List.of(randomValue()), List.of(randomValue()), List.of(randomValue()));
        try (B block = buildBlock(bigArrayBlockFactory(), expected)) {
            assertBigArrayVectorBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testBigArrayBlock() {
        List<List<V>> expected = mixedExpectedValues();
        try (B block = buildBlock(bigArrayBlockFactory(), expected)) {
            assertBigArrayBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testEmptyBlock() {
        List<List<V>> expected = List.of();
        try (B block = buildBlock(blockFactory(), expected)) {
            assertEmptyBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testAllNullBlock() {
        List<List<V>> expected = new ArrayList<>();
        expected.add(null);
        expected.add(null);
        try (B block = buildBlock(blockFactory(), expected)) {
            assertAllNullBlockRepresentation(block);
            assertBlock(block, expected);
        }
    }

    public final void testRelease() {
        List<List<V>> expected = mixedExpectedValues();
        B block = buildBlock(blockFactory(), expected);
        assertThat(breaker.getUsed(), greaterThan(0L));
        Page page = new Page(block);
        B copiedOutOfBreaker = castBlock(block.deepCopy(TestBlockFactory.getNonBreakingInstance()));
        assertValues(copiedOutOfBreaker, expected);

        block.close();
        assertThat(block.isReleased(), is(true));
        var doubleRelease = expectThrows(IllegalStateException.class, block::close);
        assertThat(doubleRelease.getMessage(), containsString("can't release already released object"));
        var readReleasedPage = expectThrows(IllegalStateException.class, () -> page.getBlock(0));
        assertThat(readReleasedPage.getMessage(), containsString("can't read released block"));
        var addReleasedBlock = expectThrows(IllegalArgumentException.class, () -> new Page(block));
        assertThat(addReleasedBlock.getMessage(), containsString("can't build page out of released blocks but"));
        assertThat(breaker.getUsed(), is(0L));
        assertValues(copiedOutOfBreaker, expected);
    }

    private List<List<V>> mixedExpectedValues() {
        List<List<V>> expected = new ArrayList<>();
        expected.add(List.of(randomValue()));
        expected.add(null);
        expected.add(List.of(randomValue(), randomValue()));
        expected.add(List.of(randomValue()));
        return expected;
    }

    protected final B buildBlock(BlockFactory blockFactory, List<List<V>> expected) {
        return buildBlock(blockFactory, expected.size(), expected);
    }

    protected final B buildBlock(BlockFactory blockFactory, int estimatedSize, List<List<V>> expected) {
        try (BB builder = createBuilder(blockFactory, estimatedSize)) {
            for (List<V> values : expected) {
                if (values == null) {
                    appendNull(builder);
                } else if (values.size() == 1) {
                    appendSingle(builder, values.get(0));
                } else {
                    appendMultivalued(builder, values);
                }
            }
            return build(builder);
        }
    }

    protected final void assertBlock(B block, List<List<V>> expected) {
        assertValues(block, expected);
        assertBlockProperties(block, expected);
        assertSlice(block, expected);
        assertFilter(block, expected);
        assertKeepMask(block, expected);
        assertInsertNulls(block, expected);
        assertDeepCopy(block, expected);
        assertBuilderCopyFrom(block, expected);
        assertLookup(block, expected);
        assertAdditionalInvariants(block, expected);
    }

    protected final List<List<V>> repeat(V value, int positions) {
        List<List<V>> expected = new ArrayList<>(positions);
        for (int p = 0; p < positions; p++) {
            expected.add(List.of(value));
        }
        return expected;
    }

    protected final List<List<V>> denseExpectedValues(int positions) {
        List<List<V>> expected = new ArrayList<>(positions);
        for (int p = 0; p < positions; p++) {
            expected.add(List.of(randomValue()));
        }
        return expected;
    }

    private void assertValues(B block, List<List<V>> expected) {
        assertThat(block.getPositionCount(), equalTo(expected.size()));
        int totalValueCount = 0;
        for (int p = 0; p < expected.size(); p++) {
            List<V> expectedValues = expected.get(p);
            int expectedValueCount = expectedValues == null ? 0 : expectedValues.size();
            totalValueCount += expectedValueCount;
            assertThat(block.isNull(p), equalTo(expectedValues == null));
            assertThat(block.getValueCount(p), equalTo(expectedValueCount));
            assertThat(valuesAt(block, p), equalTo(expectedValues));
        }
        assertThat(block.getTotalValueCount(), equalTo(totalValueCount));
        assertValueCounts(block);
    }

    private void assertBlockProperties(B block, List<List<V>> expected) {
        boolean hasNull = expected.stream().anyMatch(Objects::isNull);
        boolean allNull = expected.isEmpty() == false && expected.stream().allMatch(Objects::isNull);
        boolean hasMultivalued = expected.stream().filter(Objects::nonNull).anyMatch(v -> v.size() > 1);
        boolean denseSingleValued = expected.isEmpty() == false && expected.stream().allMatch(v -> v != null && v.size() == 1);

        if (expected.isEmpty()) {
            return;
        }
        assertThat(block.mayHaveNulls(), equalTo(hasNull));
        assertThat(block.areAllValuesNull(), equalTo(allNull));
        assertThat(block.doesHaveMultivaluedFields(), equalTo(hasMultivalued));
        if (block.mayHaveMultivaluedFields() == false) {
            assertThat(hasMultivalued, equalTo(false));
        }
        if (supportsDenseVector() && denseSingleValued) {
            Vector vector = block.asVector();
            assertThat(vector, notNullValue());
            assertThat(vector.getPositionCount(), equalTo(expected.size()));
            for (int p = 0; p < expected.size(); p++) {
                assertThat(block.getFirstValueIndex(p), equalTo(p));
            }
            vector.incRef();
            try (Block vectorBlock = vector.asBlock()) {
                assertThat(vectorBlock, equalTo(block));
            }
        } else {
            assertThat(block.asVector(), nullValue());
        }
    }

    private void assertSlice(B block, List<List<V>> expected) {
        try (Block sliced = block.slice(0, block.getPositionCount())) {
            assertThat(sliced, instanceOf(block.getClass()));
            assertValues(castBlock(sliced), expected);
        }
        try (Block sliced = block.slice(0, 0)) {
            assertValues(castBlock(sliced), List.of());
        }
        if (expected.isEmpty() == false) {
            int begin = expected.size() == 1 ? 0 : 1;
            try (Block sliced = block.slice(begin, expected.size())) {
                assertValues(castBlock(sliced), expected.subList(begin, expected.size()));
            }
        }
        if (expected.size() > 2) {
            int begin = randomIntBetween(1, expected.size() - 2);
            int end = randomIntBetween(begin + 1, expected.size() - 1);
            try (Block sliced = block.slice(begin, end)) {
                assertValues(castBlock(sliced), expected.subList(begin, end));
            }
        }
    }

    private void assertFilter(B block, List<List<V>> expected) {
        try (Block filtered = block.filter(false)) {
            assertValues(castBlock(filtered), List.of());
        }
        int[] allPositions = IntStream.range(0, expected.size()).toArray();
        try (Block filtered = block.filter(false, allPositions)) {
            assertValues(castBlock(filtered), expected);
        }
        if (expected.size() > 1) {
            int[] positions = new int[] { expected.size() - 1, 0 };
            try (Block filtered = block.filter(false, positions)) {
                List<List<V>> filteredExpected = new ArrayList<>();
                filteredExpected.add(expected.get(expected.size() - 1));
                filteredExpected.add(expected.get(0));
                assertValues(castBlock(filtered), filteredExpected);
            }
        }
        if (expected.isEmpty() == false) {
            int[] positions = new int[] { 0, 0 };
            try (Block filtered = block.filter(true, positions)) {
                List<List<V>> filteredExpected = new ArrayList<>();
                filteredExpected.add(expected.get(0));
                filteredExpected.add(expected.get(0));
                assertValues(castBlock(filtered), filteredExpected);
            }
        }
    }

    private void assertKeepMask(B block, List<List<V>> expected) {
        try (
            BooleanVector mask = block.blockFactory().newConstantBooleanVector(true, expected.size());
            Block masked = block.keepMask(mask)
        ) {
            if (masked != block && masked.asVector() != block.asVector()) {
                fail("all-true keep mask should return the original block or vector");
            }
            assertValues(castBlock(masked), expected);
        }
        try (
            BooleanVector mask = block.blockFactory().newConstantBooleanVector(false, expected.size());
            Block masked = block.keepMask(mask)
        ) {
            assertValues(castBlock(masked), allNull(expected.size()));
        }
        if (expected.size() > 1) {
            try (BooleanVector.FixedBuilder mask = block.blockFactory().newBooleanVectorFixedBuilder(expected.size())) {
                for (int p = 0; p < expected.size(); p++) {
                    mask.appendBoolean(p, p % 2 == 0);
                }
                try (BooleanVector builtMask = mask.build(); Block masked = block.keepMask(builtMask)) {
                    List<List<V>> maskedExpected = new ArrayList<>(expected.size());
                    for (int p = 0; p < expected.size(); p++) {
                        maskedExpected.add(p % 2 == 0 ? expected.get(p) : null);
                    }
                    assertValues(castBlock(masked), maskedExpected);
                }
            }
        }
    }

    private void assertDeepCopy(B block, List<List<V>> expected) {
        try (Block copy = block.deepCopy(blockFactory())) {
            assertValues(castBlock(copy), expected);
            assertThat(copy, equalTo(block));
            if (block.asVector() != null && block.asVector().isConstant()) {
                assertThat(copy.asVector() != null && copy.asVector().isConstant(), equalTo(true));
            }
        }
    }

    private void assertInsertNulls(B block, List<List<V>> expected) {
        try (IntVector.Builder beforeBuilder = block.blockFactory().newIntVectorBuilder(block.getPositionCount() + 1)) {
            List<List<V>> expectedWithNulls = new ArrayList<>(expected.size() * 2 + 1);
            for (int p = 0; p < expected.size(); p++) {
                expectedWithNulls.add(null);
                beforeBuilder.appendInt(p);
                expectedWithNulls.add(expected.get(p));
            }
            expectedWithNulls.add(null);
            beforeBuilder.appendInt(expected.size());
            try (IntVector before = beforeBuilder.build(); Block withNulls = block.insertNulls(before)) {
                assertValues(castBlock(withNulls), expectedWithNulls);
            }
        }
    }

    private void assertBuilderCopyFrom(B block, List<List<V>> expected) {
        try (BB builder = createBuilder(block.blockFactory(), expected.size())) {
            builder.copyFrom(block, 0, block.getPositionCount());
            try (B copy = build(builder)) {
                assertValues(copy, expected);
                assertThat(copy, equalTo(block));
            }
        }
    }

    private void assertLookup(B block, List<List<V>> expected) {
        if (supportsLookup() == false) {
            return;
        }
        try (IntBlock positions = positions(block.blockFactory())) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
        try (IntBlock positions = positions(block.blockFactory(), expected.size() + 1000)) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(true));
                try (Block lookedUp = lookup.next()) {
                    List<List<V>> outOfRangeExpected = new ArrayList<>();
                    outOfRangeExpected.add(null);
                    assertValues(castBlock(lookedUp), outOfRangeExpected);
                }
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
        if (expected.size() < 3 || expected.get(0) == null || expected.get(2) == null) {
            return;
        }
        try (IntBlock positions = positions(block.blockFactory(), 0, new int[] { 0, 2 }, expected.size() + 1000)) {
            try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
                assertThat(lookup.hasNext(), equalTo(true));
                try (Block lookedUp = lookup.next()) {
                    List<List<V>> lookupExpected = new ArrayList<>();
                    lookupExpected.add(expected.get(0));
                    lookupExpected.add(combine(expected.get(0), expected.get(2)));
                    lookupExpected.add(null);
                    assertValues(castBlock(lookedUp), lookupExpected);
                }
                assertThat(lookup.hasNext(), equalTo(false));
            }
        }
    }

    private static void assertValueCounts(Block block) {
        int totalValueCount = 0;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                assertThat(block.getValueCount(p), equalTo(0));
            }
            totalValueCount += block.getValueCount(p);
        }
        assertThat(block.getTotalValueCount(), equalTo(totalValueCount));
        for (int p = 0; p + 1 < block.getPositionCount(); p++) {
            if (block.isNull(p) == false) {
                assertThat(block.getValueCount(p), equalTo(block.getFirstValueIndex(p + 1) - block.getFirstValueIndex(p)));
            }
        }
    }

    private static IntBlock positions(BlockFactory blockFactory, Object... positions) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions.length)) {
            for (Object position : positions) {
                if (position instanceof Integer p) {
                    builder.appendInt(p);
                } else if (position instanceof int[] ps) {
                    builder.beginPositionEntry();
                    for (int p : ps) {
                        builder.appendInt(p);
                    }
                    builder.endPositionEntry();
                } else {
                    throw new IllegalArgumentException("invalid position [" + position + "]");
                }
            }
            return builder.build();
        }
    }

    private List<List<V>> allNull(int positionCount) {
        List<List<V>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(null);
        }
        return expected;
    }

    private List<V> combine(List<V> first, List<V> second) {
        if (first == null || second == null) {
            return null;
        }
        List<V> combined = new ArrayList<>(first.size() + second.size());
        combined.addAll(first);
        combined.addAll(second);
        return combined;
    }

    @SuppressWarnings("unchecked")
    private B castBlock(Block block) {
        return (B) block;
    }

    protected final BlockFactory blockFactory() {
        return BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService)).build();
    }

    protected final BlockFactory bigArrayBlockFactory() {
        return BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
            .maxPrimitiveArraySize(1)
            .build();
    }
}
