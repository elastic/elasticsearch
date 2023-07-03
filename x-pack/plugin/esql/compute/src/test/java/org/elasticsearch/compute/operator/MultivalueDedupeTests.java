/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class MultivalueDedupeTests extends ESTestCase {
    public static List<ElementType> supportedTypes() {
        List<ElementType> supported = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
            if (elementType == ElementType.UNKNOWN || elementType == ElementType.NULL || elementType == ElementType.DOC) {
                continue;
            }
            supported.add(elementType);
        }
        return supported;
    }

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : supportedTypes()) {
            if (elementType == ElementType.UNKNOWN || elementType == ElementType.NULL || elementType == ElementType.DOC) {
                continue;
            }
            for (boolean nullAllowed : new boolean[] { false, true }) {
                for (int max : new int[] { 10, 100, 1000 }) {
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
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(b, MultivalueDedupe.dedupeToBlockAdaptive(b.block()));
    }

    public void testDedupeViaCopyAndSort() {
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(b, MultivalueDedupe.dedupeToBlockUsingCopyAndSort(b.block()));
    }

    public void testDedupeViaCopyMissing() {
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(b, MultivalueDedupe.dedupeToBlockUsingCopyMissing(b.block()));
    }

    private BasicBlockTests.RandomBlock randomBlock() {
        return BasicBlockTests.randomBlock(
            elementType,
            positionCount,
            nullAllowed,
            minValuesPerPosition,
            maxValuesPerPosition,
            minDupsPerPosition,
            maxDupsPerPosition
        );
    }

    private void assertDeduped(BasicBlockTests.RandomBlock b, Block deduped) {
        for (int p = 0; p < b.block().getPositionCount(); p++) {
            List<Object> v = b.values().get(p);
            Matcher<? extends Object> matcher = v == null
                ? nullValue()
                : containsInAnyOrder(v.stream().collect(Collectors.toSet()).stream().sorted().toArray());
            BlockTestUtils.assertPositionValues(deduped, p, matcher);
        }
    }

    public void testHash() {
        BasicBlockTests.RandomBlock b = randomBlock();

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
        BasicBlockTests.RandomBlock b = randomBlock();

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
        int initCapacity = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());
        BasicBlockTests.RandomBlock b = randomBlock();
        BatchEncoder encoder = MultivalueDedupe.batchEncoder(b.block(), initCapacity);

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
        BasicBlockTests.RandomBlock b = randomBlock();
        BatchEncoder encoder = MultivalueDedupe.batchEncoder(b.block(), 0);

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

    private void assertBooleanHash(Set<Boolean> previousValues, BasicBlockTests.RandomBlock b) {
        boolean[] everSeen = new boolean[2];
        if (previousValues.contains(false)) {
            everSeen[0] = true;
        }
        if (previousValues.contains(true)) {
            everSeen[1] = true;
        }
        LongBlock hashes = new MultivalueDedupeBoolean((BooleanBlock) b.block()).hash(everSeen);
        List<Boolean> hashedValues = new ArrayList<>();
        if (everSeen[0]) {
            hashedValues.add(false);
        }
        if (everSeen[0]) {
            hashedValues.add(true);
        }
        assertHash(b, hashes, hashedValues.size(), previousValues, i -> hashedValues.get((int) i));
    }

    private void assertBytesRefHash(Set<BytesRef> previousValues, BasicBlockTests.RandomBlock b) {
        BytesRefHash hash = new BytesRefHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        LongBlock hashes = new MultivalueDedupeBytesRef((BytesRefBlock) b.block()).hash(hash);
        assertHash(b, hashes, hash.size(), previousValues, i -> hash.get(i, new BytesRef()));
    }

    private void assertIntHash(Set<Integer> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        LongBlock hashes = new MultivalueDedupeInt((IntBlock) b.block()).hash(hash);
        assertHash(b, hashes, hash.size(), previousValues, i -> (int) hash.get(i));
    }

    private void assertLongHash(Set<Long> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        LongBlock hashes = new MultivalueDedupeLong((LongBlock) b.block()).hash(hash);
        assertHash(b, hashes, hash.size(), previousValues, i -> hash.get(i));
    }

    private void assertDoubleHash(Set<Double> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(d -> hash.add(Double.doubleToLongBits(d)));
        LongBlock hashes = new MultivalueDedupeDouble((DoubleBlock) b.block()).hash(hash);
        assertHash(b, hashes, hash.size(), previousValues, i -> Double.longBitsToDouble(hash.get(i)));
    }

    private void assertHash(
        BasicBlockTests.RandomBlock b,
        LongBlock hashes,
        long hashSize,
        Set<? extends Object> previousValues,
        LongFunction<Object> lookup
    ) {
        Set<Object> allValues = new HashSet<>();
        allValues.addAll(previousValues);
        for (int p = 0; p < b.block().getPositionCount(); p++) {
            int count = hashes.getValueCount(p);
            List<Object> v = b.values().get(p);
            if (v == null) {
                assertThat(hashes.isNull(p), equalTo(true));
                assertThat(count, equalTo(0));
                return;
            }
            List<Object> actualValues = new ArrayList<>(count);
            int start = hashes.getFirstValueIndex(p);
            int end = start + count;
            for (int i = start; i < end; i++) {
                actualValues.add(lookup.apply(hashes.getLong(i)));
            }
            assertThat(actualValues, containsInAnyOrder(v.stream().collect(Collectors.toSet()).stream().sorted().toArray()));
            allValues.addAll(v);
        }

        Set<Object> hashedValues = new HashSet<>((int) hashSize);
        for (long i = 0; i < hashSize; i++) {
            hashedValues.add(lookup.apply(i));
        }
        assertThat(hashedValues, equalTo(allValues));
    }

    private int assertEncodedPosition(BasicBlockTests.RandomBlock b, BatchEncoder encoder, int position, int offset, int valueOffset) {
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
        Block.Builder builder = elementType.newBlockBuilder(encoder.valueCount(offset));
        BytesRef[] toDecode = new BytesRef[encoder.valueCount(offset)];
        for (int i = 0; i < toDecode.length; i++) {
            toDecode[i] = encoder.read(valueOffset++, new BytesRef());
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
        BatchEncoder.decoder(elementType).decode(builder, toDecode, toDecode.length);
        for (int i = 0; i < toDecode.length; i++) {
            assertThat(toDecode[i].length, equalTo(0));
        }
        Block decoded = builder.build();
        assertThat(decoded.getPositionCount(), equalTo(toDecode.length));
        List<Object> actual = new ArrayList<>();
        BasicBlockTests.valuesAtPositions(decoded, 0, decoded.getPositionCount())
            .stream()
            .forEach(l -> actual.add(l == null ? null : l.get(0)));
        Collections.sort(actual, Comparator.comparing(o -> {
            @SuppressWarnings("unchecked") // This is totally comparable, believe me
            var c = (Comparable<Object>) o;
            return c;
        })); // Sort for easier visual comparison of errors
        assertThat(actual, equalTo(expected));
        return valueOffset;
    }
}
