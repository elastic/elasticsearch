/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultivalueDedupeTests extends ESTestCase {
    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        for (ElementType elementType : ElementType.values()) {
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
}
