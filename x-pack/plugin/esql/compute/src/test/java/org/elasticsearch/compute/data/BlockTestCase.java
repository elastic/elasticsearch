/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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

    protected abstract ElementType expectedElementType();

    protected TransportVersion minimumSerializationTransportVersion() {
        return TransportVersion.minimumCompatible();
    }

    protected boolean supportsLookup() {
        return true;
    }

    protected boolean supportsExpand() {
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

    public final void testMvOrdering() {
        List<List<V>> expected = List.of(List.of(randomValue(), randomValue()));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            try (B block = buildBlock(blockFactory(), ordering, expected)) {
                assertThat(block.mvOrdering(), equalTo(ordering));
                switch (ordering) {
                    case UNORDERED -> {
                        assertFalse(block.mvDeduplicated());
                        assertFalse(block.mvSortedAscending());
                    }
                    case DEDUPLICATED_UNORDERD -> {
                        assertTrue(block.mvDeduplicated());
                        assertFalse(block.mvSortedAscending());
                    }
                    case SORTED_ASCENDING -> {
                        assertFalse(block.mvDeduplicated());
                        assertTrue(block.mvSortedAscending());
                    }
                    case DEDUPLICATED_AND_SORTED_ASCENDING -> {
                        assertTrue(block.mvDeduplicated());
                        assertTrue(block.mvSortedAscending());
                    }
                }
            }
        }
        try (B block = buildBlock(blockFactory(), List.of(List.of(randomValue())))) {
            assertTrue(block.mvDeduplicated());
            assertTrue(block.mvSortedAscending());
        }
    }

    public final void testRefCounting() {
        B block = buildBlock(blockFactory(), mixedExpectedValues());
        try {
            assertThat(breaker.getUsed(), greaterThan(0L));
            assertTrue(block.hasReferences());
            int extraReferences = randomIntBetween(1, 15);
            for (int i = 0; i < extraReferences; i++) {
                if (randomBoolean()) {
                    block.incRef();
                } else {
                    assertTrue(block.tryIncRef());
                }
            }
            for (int i = 0; i < extraReferences; i++) {
                if (randomBoolean()) {
                    block.close();
                } else {
                    assertFalse(block.decRef());
                }
                assertTrue(block.hasReferences());
            }
            if (randomBoolean()) {
                block.close();
            } else {
                assertTrue(block.decRef());
            }
            assertFalse(block.hasReferences());
            assertFalse(block.tryIncRef());
            expectThrows(IllegalStateException.class, block::close);
            expectThrows(IllegalStateException.class, block::incRef);
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            // Drain leftover refs so @After checkBreaker doesn't mask the real failure.
            while (block.hasReferences()) {
                block.close();
            }
        }
    }

    public final void testAttachReleasable() {
        AtomicInteger releases = new AtomicInteger();
        B block = buildBlock(blockFactory(), mixedExpectedValues());
        block.attachReleasable(releases::incrementAndGet);
        block.incRef();
        block.decRef();
        assertThat(releases.get(), equalTo(0));
        block.close();
        assertThat(releases.get(), equalTo(1));

        B doubleAttach = buildBlock(blockFactory(), mixedExpectedValues());
        doubleAttach.attachReleasable(() -> {});
        expectThrows(IllegalStateException.class, () -> doubleAttach.attachReleasable(() -> {}));
        doubleAttach.close();

        B attachNull = buildBlock(blockFactory(), mixedExpectedValues());
        expectThrows(NullPointerException.class, () -> attachNull.attachReleasable(null));
        attachNull.close();

        B attachAfterRelease = buildBlock(blockFactory(), mixedExpectedValues());
        attachAfterRelease.close();
        expectThrows(IllegalStateException.class, () -> attachAfterRelease.attachReleasable(() -> {}));
    }

    public final void testAllowPassingToDifferentDriver() {
        BlockFactory parentFactory = blockFactory();
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(parentFactory.breaker(), 1024, 1024);
        BlockFactory childFactory = parentFactory.newChildFactory(localBreaker);
        try {
            try (B block = buildBlock(childFactory, mixedExpectedValues())) {
                assertThat(block.blockFactory(), sameInstance(childFactory));
                block.allowPassingToDifferentDriver();
                assertThat(block.blockFactory(), sameInstance(parentFactory));
            }
        } finally {
            localBreaker.close();
        }
    }

    /**
     * Verifies that large lookups are split into multiple blocks without changing their values or block factory.
     */
    public final void testLookupChunking() {
        assumeTrue("lookup unsupported", supportsLookup());
        int positionCount = Operator.MIN_TARGET_PAGE_SIZE * 3;
        List<List<V>> expected = denseExpectedValues(positionCount);
        BlockFactory factory = blockFactory();
        try (B block = buildBlock(factory, expected); IntBlock.Builder positionsBuilder = factory.newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                positionsBuilder.appendInt(p);
            }
            try (IntBlock positions = positionsBuilder.build()) {
                int offset = 0;
                int chunks = 0;
                try (ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                    while (lookup.hasNext()) {
                        try (Block chunk = lookup.next()) {
                            assertThat(chunk.blockFactory(), sameInstance(positions.blockFactory()));
                            int end = offset + chunk.getPositionCount();
                            assertValues(castBlock(chunk), expected.subList(offset, end));
                            offset = end;
                            chunks++;
                        }
                    }
                }
                assertThat(offset, equalTo(positionCount));
                assertThat(chunks, greaterThan(1));
            }
        }
    }

    public final void testSerialization() throws IOException {
        List<List<V>> expected = mixedExpectedValues();
        try (B block = buildBlock(blockFactory(), expected)) {
            assertSerializationAtSupportedVersions(block, expected);
        }
    }

    public final void testConstantBlockSerialization() throws IOException {
        assumeTrue("constant block factory unsupported", supportsConstantBlockFactory());
        V value = randomValue();
        int positions = randomIntBetween(1, 8192);
        try (B block = createConstantBlock(blockFactory(), value, positions)) {
            assertSerializationAtSupportedVersions(block, repeat(value, positions), copy -> {
                assertThat(copy.asVector(), notNullValue());
                assertTrue(copy.asVector().isConstant());
            });
        }
    }

    public final void testEmptyBlockSerialization() throws IOException {
        try (B block = buildBlock(blockFactory(), List.of())) {
            assertSerializationAtSupportedVersions(block, List.of());
        }
        try (B source = buildBlock(blockFactory(), allNull(1)); Block filtered = source.filter(false)) {
            assertSerializationAtSupportedVersions(castBlock(filtered), List.of());
        }
        try (B source = buildBlock(blockFactory(), List.of(List.of(randomValue()))); Block filtered = source.filter(false)) {
            assertSerializationAtSupportedVersions(castBlock(filtered), List.of());
        }
    }

    public final void testFilteredBlockSerialization() throws IOException {
        List<List<V>> denseExpected = List.of(List.of(randomValue()), List.of(randomValue()));
        try (B source = buildBlock(blockFactory(), denseExpected); Block filtered = source.filter(false, 1)) {
            assertSerializationAtSupportedVersions(castBlock(filtered), List.of(denseExpected.get(1)));
        }

        List<List<V>> sparseExpected = new ArrayList<>();
        sparseExpected.add(List.of(randomValue()));
        sparseExpected.add(null);
        try (B source = buildBlock(blockFactory(), sparseExpected); Block filtered = source.filter(false, 0)) {
            assertSerializationAtSupportedVersions(castBlock(filtered), List.of(sparseExpected.get(0)));
        }
    }

    private void assertSerializationAtSupportedVersions(B block, List<List<V>> expected) throws IOException {
        assertSerializationAtSupportedVersions(block, expected, copy -> {});
    }

    private void assertSerializationAtSupportedVersions(B block, List<List<V>> expected, Consumer<B> extra) throws IOException {
        BlockFactory factory = block.blockFactory();
        assertSerialization(block, factory, expected, minimumSerializationTransportVersion(), extra);
        assertSerialization(
            block,
            factory,
            expected,
            TransportVersionUtils.randomVersionSupporting(minimumSerializationTransportVersion()),
            extra
        );
        assertSerialization(block, factory, expected, TransportVersion.current(), extra);
    }

    private void assertSerialization(B block, BlockFactory factory, List<List<V>> expected, TransportVersion version, Consumer<B> extra)
        throws IOException {
        try (B copy = serializeDeserialize(block, factory, version)) {
            assertValues(copy, expected);
            assertThat(copy.elementType(), equalTo(expectedElementType()));
            assertThat(copy.blockFactory(), sameInstance(factory));
            extra.accept(copy);
        }
    }

    public final void testAccounting() {
        try (B block = buildBlock(blockFactory(), mixedExpectedValues())) {
            assertThat(block.ramBytesUsed(), greaterThan(0L));
            assertThat(block.blockFactory().breaker().getUsed(), greaterThanOrEqualTo(block.ramBytesUsed()));
        }
    }

    public final void testCrankyFilter() {
        testCrankyOperation("filter", this::assertFilter);
    }

    public final void testCrankyKeepMask() {
        testCrankyOperation("keepMask", this::assertKeepMask);
    }

    public final void testCrankyLookup() {
        testCrankyOperation("lookup", this::assertLookup);
    }

    public final void testCrankySlice() {
        testCrankyOperation("slice", this::assertSlice);
    }

    public final void testCrankyExpand() {
        testCrankyOperation("expand", this::assertExpand);
    }

    public final void testCrankyInsertNulls() {
        testCrankyOperation("insertNulls", this::assertInsertNulls);
    }

    public final void testCrankyDeepCopy() {
        testCrankyOperation("deepCopy", this::assertDeepCopy);
    }

    private void testCrankyOperation(String operation, BiConsumer<B, List<List<V>>> run) {
        testCrankyOperation(operation, run, false);
        testCrankyOperation(operation, run, true);
    }

    private void testCrankyOperation(String operation, BiConsumer<B, List<List<V>>> run, boolean useBigArrays) {
        CrankyCircuitBreakerService breakerService = new CrankyCircuitBreakerService();
        BlockFactoryBuilder factoryBuilder = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking()
        );
        if (useBigArrays) {
            factoryBuilder.maxPrimitiveArraySize(1);
        }
        BlockFactory crankyFactory = factoryBuilder.build();
        CircuitBreaker crankyBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        for (int i = 0; i < 100; i++) {
            List<List<V>> expected = mixedExpectedValues();
            try {
                try (B block = buildBlock(crankyFactory, expected)) {
                    run.accept(block, expected);
                }
            } catch (CircuitBreakingException e) {
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
            assertThat(operation + (useBigArrays ? " with big arrays" : ""), crankyBreaker.getUsed(), equalTo(0L));
        }
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
            appendValues(builder, expected);
            B block = build(builder);
            assertThat(block.blockFactory(), sameInstance(blockFactory));
            return block;
        }
    }

    private B buildBlock(BlockFactory blockFactory, Block.MvOrdering ordering, List<List<V>> expected) {
        try (BB builder = createBuilder(blockFactory, expected.size())) {
            builder.mvOrdering(ordering);
            appendValues(builder, expected);
            B block = build(builder);
            assertThat(block.blockFactory(), sameInstance(blockFactory));
            return block;
        }
    }

    private void appendValues(BB builder, List<List<V>> expected) {
        for (List<V> values : expected) {
            if (values == null) {
                appendNull(builder);
            } else if (values.size() == 1) {
                appendSingle(builder, values.get(0));
            } else {
                appendMultivalued(builder, values);
            }
        }
    }

    protected final void assertBlock(B block, List<List<V>> expected) {
        assertValues(block, expected);
        assertBlockProperties(block, expected);
        assertSlice(block, expected);
        assertFilter(block, expected);
        assertExpand(block, expected);
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
        assertThat(block.elementType(), equalTo(expectedElementType()));
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
            // Full-range slice reuses the block; *VectorBlock wraps via asBlock() so only the vector is reused.
            Vector vector = block.asVector();
            if (vector != null) {
                assertThat(sliced.asVector(), sameInstance(vector));
            } else {
                assertThat(sliced, sameInstance(block));
            }
            assertValues(castBlock(sliced), expected);
        }
        assertTrue(block.hasReferences());
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
        if (expected.size() > 2) {
            int[] positions = IntStream.range(0, expected.size()).toArray();
            int offset = randomIntBetween(1, positions.length - 2);
            int length = randomIntBetween(1, positions.length - offset);
            try (Block filtered = block.filter(false, positions, offset, length)) {
                assertValues(castBlock(filtered), expected.subList(offset, offset + length));
            }
        }
    }

    private void assertExpand(B block, List<List<V>> expected) {
        if (supportsExpand() == false) {
            return;
        }
        List<List<V>> expandedExpected = new ArrayList<>();
        for (List<V> values : expected) {
            if (values == null) {
                expandedExpected.add(null);
            } else {
                for (V value : values) {
                    expandedExpected.add(List.of(value));
                }
            }
        }
        try (Block expanded = block.expand()) {
            if (block.mayHaveMultivaluedFields() == false) {
                assertThat(expanded, sameInstance(block));
            }
            assertValues(castBlock(expanded), expandedExpected);
        }
        assertTrue(block.hasReferences());
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
        // Distinct factory, same breaker: verifies deepCopy ownership without losing cranky CB coverage.
        BlockFactory sourceFactory = block.blockFactory();
        BlockFactory targetFactory = new BlockFactory(sourceFactory.breaker(), sourceFactory.bigArrays());
        try (Block copy = block.deepCopy(targetFactory)) {
            assertValues(castBlock(copy), expected);
            assertThat(copy, equalTo(block));
            assertThat(copy.blockFactory(), sameInstance(targetFactory));
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
                    assertThat(lookedUp.blockFactory(), sameInstance(positions.blockFactory()));
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
                    assertThat(lookedUp.blockFactory(), sameInstance(positions.blockFactory()));
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
    private B serializeDeserialize(B block, BlockFactory blockFactory, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            Block.writeTypedBlock(block, out);
            try (
                BlockStreamInput in = new BlockStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), blockFactory)
            ) {
                in.setTransportVersion(version);
                return (B) Block.readTypedBlock(in);
            }
        }
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
