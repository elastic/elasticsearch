/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MultivalueDedupeTests extends ESTestCase {
    public static List<ElementType> supportedTypes() {
        List<ElementType> supported = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
            if (oneOf(
                elementType,
                ElementType.UNKNOWN,
                ElementType.DOC,
                ElementType.COMPOSITE,
                ElementType.FLOAT,
                ElementType.AGGREGATE_METRIC_DOUBLE
            )) {
                continue;
            }
            supported.add(elementType);
        }
        return supported;
    }

    private static boolean oneOf(ElementType elementType, ElementType... others) {
        for (ElementType other : others) {
            if (elementType == other) {
                return true;
            }
        }
        return false;
    }

    @ParametersFactory(argumentFormatting = "elementType=%s positions=%s nullsAllowed=%s valuesPerPosition=%s-%s dupsPerPosition=%s-%s")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : supportedTypes()) {
            for (boolean nullAllowed : new boolean[] { false, true }) {
                for (int max : new int[] { 10, 100, 1000 }) {
                    params.add(new Object[] { elementType, 1000, nullAllowed, 1, 1, 0, 0 });
                    params.add(new Object[] { elementType, 1000, nullAllowed, 1, max, 0, 0 });
                    params.add(new Object[] { elementType, 1000, nullAllowed, 1, max, 0, 100 });
                }
            }
        }
        return params;
    }

    private final ElementType elementType;
    private final int positionCount;
    private final boolean nullAllowed;
    private final int minValuesPerPosition;
    private final int maxValuesPerPosition;
    private final int minDupsPerPosition;
    private final int maxDupsPerPosition;

    public MultivalueDedupeTests(
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        this.elementType = elementType;
        this.positionCount = positionCount;
        this.nullAllowed = nullAllowed;
        this.minValuesPerPosition = minValuesPerPosition;
        this.maxValuesPerPosition = maxValuesPerPosition;
        this.minDupsPerPosition = minDupsPerPosition;
        this.maxDupsPerPosition = maxDupsPerPosition;
    }

    public void testDedupeAdaptive() {
        BlockFactory blockFactory = blockFactory();
        RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockAdaptive(b.block(), blockFactory));
    }

    public void testDedupeViaCopyAndSort() {
        BlockFactory blockFactory = blockFactory();
        RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockUsingCopyAndSort(b.block(), blockFactory));
    }

    public void testDedupeViaCopyMissing() {
        BlockFactory blockFactory = blockFactory();
        RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockUsingCopyMissing(b.block(), blockFactory));
    }

    private RandomBlock randomBlock() {
        return RandomBlock.randomBlock(
            elementType,
            positionCount,
            elementType == ElementType.NULL ? true : nullAllowed,
            minValuesPerPosition,
            maxValuesPerPosition,
            minDupsPerPosition,
            maxDupsPerPosition
        );
    }

    private void assertDeduped(BlockFactory blockFactory, RandomBlock b, Block dedupedBlock) {
        try {
            if (dedupedBlock != b.block()) {
                assertThat(dedupedBlock.blockFactory(), sameInstance(blockFactory));
            }
            for (int p = 0; p < b.block().getPositionCount(); p++) {
                List<Object> v = b.values().get(p);
                Matcher<? extends Object> matcher = v == null
                    ? nullValue()
                    : containsInAnyOrder(v.stream().collect(Collectors.toSet()).stream().sorted().toArray());
                BlockTestUtils.assertPositionValues(dedupedBlock, p, matcher);
            }
        } finally {
            Releasables.closeExpectNoException(dedupedBlock);
        }
    }

    public void testHash() {
        assumeFalse("not hash for null", elementType == ElementType.NULL);
        RandomBlock b = randomBlock();
        switch (b.block().elementType()) {
            case BOOLEAN -> assertBooleanHash(Set.of(), b);
            case BYTES_REF -> assertBytesRefHash(Set.of(), b);
            case INT -> assertIntHash(Set.of(), b);
            case LONG -> assertLongHash(Set.of(), b);
            case DOUBLE -> assertDoubleHash(Set.of(), b);
            default -> throw new IllegalArgumentException();
        }
    }

    public void testHashWithPreviousValues() {
        assumeFalse("not hash for null", elementType == ElementType.NULL);
        RandomBlock b = randomBlock();
        switch (b.block().elementType()) {
            case BOOLEAN -> {
                Set<Boolean> previousValues = switch (between(0, 2)) {
                    case 0 -> Set.of(false);
                    case 1 -> Set.of(true);
                    case 2 -> Set.of(false, true);
                    default -> throw new IllegalArgumentException();
                };
                assertBooleanHash(previousValues, b);
            }
            case BYTES_REF -> {
                // TODO: Also test spatial WKB
                int prevSize = between(1, 10000);
                Set<BytesRef> previousValues = new HashSet<>(prevSize);
                while (previousValues.size() < prevSize) {
                    previousValues.add(new BytesRef(randomAlphaOfLengthBetween(1, 20)));
                }
                assertBytesRefHash(previousValues, b);
            }
            case INT -> {
                int prevSize = between(1, 10000);
                Set<Integer> previousValues = new HashSet<>(prevSize);
                while (previousValues.size() < prevSize) {
                    previousValues.add(randomInt());
                }
                assertIntHash(previousValues, b);
            }
            case LONG -> {
                int prevSize = between(1, 10000);
                Set<Long> previousValues = new HashSet<>(prevSize);
                while (previousValues.size() < prevSize) {
                    previousValues.add(randomLong());
                }
                assertLongHash(previousValues, b);
            }
            case DOUBLE -> {
                int prevSize = between(1, 10000);
                Set<Double> previousValues = new HashSet<>(prevSize);
                while (previousValues.size() < prevSize) {
                    previousValues.add(randomDouble());
                }
                assertDoubleHash(previousValues, b);
            }
            default -> throw new IllegalArgumentException();
        }
    }

    public void testBatchEncodeAll() {
        assumeFalse("null only direct encodes", elementType == ElementType.NULL);
        int initCapacity = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());
        RandomBlock b = randomBlock();
        var encoder = (BatchEncoder.MVEncoder) MultivalueDedupe.batchEncoder(b.block(), initCapacity, false);

        int valueOffset = 0;
        for (int p = 0, positionOffset = Integer.MAX_VALUE; p < b.block().getPositionCount(); p++, positionOffset++) {
            while (positionOffset >= encoder.positionCount()) {
                encoder.encodeNextBatch();
                positionOffset = 0;
                valueOffset = 0;
            }
            assertThat(encoder.bytesCapacity(), greaterThanOrEqualTo(initCapacity));
            valueOffset = assertEncodedPosition(b, encoder, p, positionOffset, valueOffset);
        }
    }

    public void testBatchEncoderStartSmall() {
        assumeFalse("Booleans don't grow in the same way", elementType == ElementType.BOOLEAN);
        assumeFalse("Nulls don't grow", elementType == ElementType.NULL);
        RandomBlock b = randomBlock();
        var encoder = (BatchEncoder.MVEncoder) MultivalueDedupe.batchEncoder(b.block(), 0, false);

        /*
         * We run can't fit the first non-null position into our 0 bytes.
         * *unless we're doing booleans, those don't bother with expanding
         * and go with a minimum block size of 2.
         */
        int leadingNulls = 0;
        while (leadingNulls < b.values().size() && b.values().get(leadingNulls) == null) {
            leadingNulls++;
        }
        encoder.encodeNextBatch();
        assertThat(encoder.bytesLength(), equalTo(0));
        assertThat(encoder.positionCount(), equalTo(leadingNulls));

        /*
         * When we add against we scale the array up to handle at least one position.
         * We may get more than one position because the scaling oversizes the destination
         * and may end up with enough extra room to fit more trailing positions.
         */
        encoder.encodeNextBatch();
        assertThat(encoder.bytesLength(), greaterThan(0));
        assertThat(encoder.positionCount(), greaterThanOrEqualTo(1));
        assertThat(encoder.firstPosition(), equalTo(leadingNulls));
        assertEncodedPosition(b, encoder, leadingNulls, 0, 0);
    }

    private void assertBooleanHash(Set<Boolean> previousValues, RandomBlock b) {
        boolean[] everSeen = new boolean[3];
        if (previousValues.contains(false)) {
            everSeen[1] = true;
        }
        if (previousValues.contains(true)) {
            everSeen[2] = true;
        }
        try (IntBlock hashes = new MultivalueDedupeBoolean((BooleanBlock) b.block()).hash(blockFactory(), everSeen)) {
            List<Boolean> hashedValues = new ArrayList<>();
            if (everSeen[1]) {
                hashedValues.add(false);
            }
            if (everSeen[2]) {
                hashedValues.add(true);
            }
            assertHash(b, hashes, hashedValues.size(), previousValues, i -> hashedValues.get((int) i));
        }
    }

    private void assertBytesRefHash(Set<BytesRef> previousValues, RandomBlock b) {
        BytesRefHash hash = new BytesRefHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.forEach(hash::add);
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeBytesRef((BytesRefBlock) b.block()).hashAdd(blockFactory(), hash);
        try (IntBlock ords = hashes.ords()) {
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
            assertHash(b, ords, hash.size(), previousValues, i -> hash.get(i, new BytesRef()));
            long sizeBeforeLookup = hash.size();
            try (IntBlock lookup = new MultivalueDedupeBytesRef((BytesRefBlock) b.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, b, lookup, i -> hash.get(i, new BytesRef()));
            }
            RandomBlock other = randomBlock();
            if (randomBoolean()) {
                other = b.merge(other);
            }
            try (IntBlock lookup = new MultivalueDedupeBytesRef((BytesRefBlock) other.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, other, lookup, i -> hash.get(i, new BytesRef()));
            }
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
        }
    }

    private void assertIntHash(Set<Integer> previousValues, RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.forEach(hash::add);
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeInt((IntBlock) b.block()).hashAdd(blockFactory(), hash);
        try (IntBlock ords = hashes.ords()) {
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
            assertHash(b, ords, hash.size(), previousValues, i -> (int) hash.get(i));
            long sizeBeforeLookup = hash.size();
            try (IntBlock lookup = new MultivalueDedupeInt((IntBlock) b.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, b, lookup, i -> (int) hash.get(i));
            }
            RandomBlock other = randomBlock();
            if (randomBoolean()) {
                other = b.merge(other);
            }
            try (IntBlock lookup = new MultivalueDedupeInt((IntBlock) other.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, other, lookup, i -> (int) hash.get(i));
            }
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
        }
    }

    private void assertLongHash(Set<Long> previousValues, RandomBlock b) {
        try (LongHash hash = new LongHash(1, blockFactory().bigArrays())) {
            previousValues.forEach(hash::add);
            MultivalueDedupe.HashResult hashes = new MultivalueDedupeLong((LongBlock) b.block()).hashAdd(blockFactory(), hash);
            try (IntBlock ords = hashes.ords()) {
                assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
                assertHash(b, ords, hash.size(), previousValues, hash::get);
                long sizeBeforeLookup = hash.size();
                try (IntBlock lookup = new MultivalueDedupeLong((LongBlock) b.block()).hashLookup(blockFactory(), hash)) {
                    assertThat(hash.size(), equalTo(sizeBeforeLookup));
                    assertLookup(previousValues, b, b, lookup, hash::get);
                }
                RandomBlock other = randomBlock();
                if (randomBoolean()) {
                    other = b.merge(other);
                }
                try (IntBlock lookup = new MultivalueDedupeLong((LongBlock) other.block()).hashLookup(blockFactory(), hash)) {
                    assertThat(hash.size(), equalTo(sizeBeforeLookup));
                    assertLookup(previousValues, b, other, lookup, hash::get);
                }
                assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
            }
        }
    }

    private void assertDoubleHash(Set<Double> previousValues, RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.forEach(d -> hash.add(Double.doubleToLongBits(d)));
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeDouble((DoubleBlock) b.block()).hashAdd(blockFactory(), hash);
        try (IntBlock ords = hashes.ords()) {
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
            assertHash(b, ords, hash.size(), previousValues, i -> Double.longBitsToDouble(hash.get(i)));
            long sizeBeforeLookup = hash.size();
            try (IntBlock lookup = new MultivalueDedupeDouble((DoubleBlock) b.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, b, lookup, i -> Double.longBitsToDouble(hash.get(i)));
            }
            RandomBlock other = randomBlock();
            if (randomBoolean()) {
                other = b.merge(other);
            }
            try (IntBlock lookup = new MultivalueDedupeDouble((DoubleBlock) other.block()).hashLookup(blockFactory(), hash)) {
                assertThat(hash.size(), equalTo(sizeBeforeLookup));
                assertLookup(previousValues, b, other, lookup, i -> Double.longBitsToDouble(hash.get(i)));
            }
            assertThat(hashes.sawNull(), equalTo(shouldHaveSeenNull(b)));
        }
    }

    private Boolean shouldHaveSeenNull(RandomBlock b) {
        return b.values().stream().anyMatch(Objects::isNull);
    }

    private void assertHash(
        RandomBlock b,
        IntBlock hashes,
        long hashSize,
        Set<? extends Object> previousValues,
        LongFunction<Object> valueLookup
    ) {
        Set<Object> allValues = new HashSet<>();
        allValues.addAll(previousValues);
        for (int p = 0; p < b.block().getPositionCount(); p++) {
            assertThat(hashes.isNull(p), equalTo(false));
            int count = hashes.getValueCount(p);
            int start = hashes.getFirstValueIndex(p);
            List<Object> v = b.values().get(p);
            if (v == null) {
                assertThat(count, equalTo(1));
                assertThat(hashes.getInt(start), equalTo(0));
                return;
            }
            Set<Object> actualValues = new TreeSet<>();
            int end = start + count;
            for (int i = start; i < end; i++) {
                actualValues.add(valueLookup.apply(hashes.getInt(i) - 1));
            }
            assertThat(actualValues, equalTo(new TreeSet<>(v)));
            allValues.addAll(v);
        }

        Set<Object> hashedValues = new HashSet<>((int) hashSize);
        for (long i = 0; i < hashSize; i++) {
            hashedValues.add(valueLookup.apply(i));
        }
        assertThat(hashedValues, equalTo(allValues));
    }

    private void assertLookup(
        Set<? extends Object> previousValues,
        RandomBlock hashed,
        RandomBlock lookedUp,
        IntBlock lookup,
        LongFunction<Object> valueLookup
    ) {
        Set<Object> contained = new HashSet<>(previousValues);
        for (List<Object> values : hashed.values()) {
            if (values != null) {
                contained.addAll(values);
            }
        }
        for (int p = 0; p < lookedUp.block().getPositionCount(); p++) {
            int count = lookup.getValueCount(p);
            int start = lookup.getFirstValueIndex(p);
            List<Object> v = lookedUp.values().get(p);
            if (v == null) {
                assertThat(count, equalTo(1));
                assertThat(lookup.getInt(start), equalTo(0));
                return;
            }
            Set<Object> actualValues = new TreeSet<>();
            int end = start + count;
            for (int i = start; i < end; i++) {
                actualValues.add(valueLookup.apply(lookup.getInt(i) - 1));
            }
            // System.err.println(actualValues + " " +
            // v.stream().filter(contained::contains).collect(Collectors.toCollection(TreeSet::new)));
            assertThat(actualValues, equalTo(v.stream().filter(contained::contains).collect(Collectors.toCollection(TreeSet::new))));
        }
    }

    private int assertEncodedPosition(RandomBlock b, BatchEncoder encoder, int position, int offset, int valueOffset) {
        List<Object> expected = b.values().get(position);
        if (expected == null) {
            expected = new ArrayList<>();
            expected.add(null);
            // BatchEncoder encodes null as a special empty value, but it counts as a value
        } else {
            NavigableSet<Object> set = new TreeSet<>();
            set.addAll(expected);
            expected = new ArrayList<>(set);
        }

        /*
         * Decode all values at the positions into a block so we can compare them easily.
         * This produces a block with a single value per position, but it's good enough
         * for comparison.
         */
        Block.Builder builder = elementType.newBlockBuilder(encoder.valueCount(offset), TestBlockFactory.getNonBreakingInstance());
        BytesRef[] toDecode = new BytesRef[encoder.valueCount(offset)];
        for (int i = 0; i < toDecode.length; i++) {
            BytesRefBuilder dest = new BytesRefBuilder();
            encoder.read(valueOffset++, dest);
            toDecode[i] = dest.toBytesRef();
            if (b.values().get(position) == null) {
                // Nulls are encoded as 0 length values
                assertThat(toDecode[i].length, equalTo(0));
            } else {
                switch (elementType) {
                    case INT -> assertThat(toDecode[i].length, equalTo(Integer.BYTES));
                    case LONG -> assertThat(toDecode[i].length, equalTo(Long.BYTES));
                    case DOUBLE -> assertThat(toDecode[i].length, equalTo(Double.BYTES));
                    case BOOLEAN -> assertThat(toDecode[i].length, equalTo(1));
                    case BYTES_REF -> {
                        // Not a well defined length
                    }
                    default -> fail("unsupported type");
                }
            }
        }
        BatchEncoder.decoder(elementType).decode(builder, i -> toDecode[i].length == 0, toDecode, toDecode.length);
        for (int i = 0; i < toDecode.length; i++) {
            assertThat(toDecode[i].length, equalTo(0));
        }
        Block decoded = builder.build();
        assertThat(decoded.getPositionCount(), equalTo(toDecode.length));
        List<Object> actual = new ArrayList<>();
        valuesAtPositions(decoded, 0, decoded.getPositionCount()).stream().forEach(l -> actual.add(l == null ? null : l.get(0)));
        Collections.sort(actual, Comparator.comparing(o -> {
            @SuppressWarnings("unchecked") // This is totally comparable, believe me
            var c = (Comparable<Object>) o;
            return c;
        })); // Sort for easier visual comparison of errors
        assertThat(actual, equalTo(expected));
        return valueOffset;
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private BlockFactory blockFactory() {
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new BlockFactory(breaker, bigArrays);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
