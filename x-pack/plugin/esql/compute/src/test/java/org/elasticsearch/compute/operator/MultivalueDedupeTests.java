/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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
        BlockFactory blockFactory = blockFactory();
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockAdaptive(Block.Ref.floating(b.block()), blockFactory));
    }

    public void testDedupeViaCopyAndSort() {
        BlockFactory blockFactory = blockFactory();
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockUsingCopyAndSort(Block.Ref.floating(b.block()), blockFactory));
    }

    public void testDedupeViaCopyMissing() {
        BlockFactory blockFactory = blockFactory();
        BasicBlockTests.RandomBlock b = randomBlock();
        assertDeduped(blockFactory, b, MultivalueDedupe.dedupeToBlockUsingCopyMissing(Block.Ref.floating(b.block()), blockFactory));
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

    private void assertDeduped(BlockFactory blockFactory, BasicBlockTests.RandomBlock b, Block.Ref deduped) {
        try (Block dedupedBlock = deduped.block()) {
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
        boolean[] everSeen = new boolean[3];
        if (previousValues.contains(false)) {
            everSeen[1] = true;
        }
        if (previousValues.contains(true)) {
            everSeen[2] = true;
        }
        IntBlock hashes = new MultivalueDedupeBoolean(Block.Ref.floating(b.block())).hash(everSeen);
        List<Boolean> hashedValues = new ArrayList<>();
        if (everSeen[1]) {
            hashedValues.add(false);
        }
        if (everSeen[2]) {
            hashedValues.add(true);
        }
        assertHash(b, hashes, hashedValues.size(), previousValues, i -> hashedValues.get((int) i));
    }

    private void assertBytesRefHash(Set<BytesRef> previousValues, BasicBlockTests.RandomBlock b) {
        BytesRefHash hash = new BytesRefHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeBytesRef(Block.Ref.floating(b.block())).hash(hash);
        assertThat(hashes.sawNull(), equalTo(b.values().stream().anyMatch(v -> v == null)));
        assertHash(b, hashes.ords(), hash.size(), previousValues, i -> hash.get(i, new BytesRef()));
    }

    private void assertIntHash(Set<Integer> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeInt(Block.Ref.floating(b.block())).hash(hash);
        assertThat(hashes.sawNull(), equalTo(b.values().stream().anyMatch(v -> v == null)));
        assertHash(b, hashes.ords(), hash.size(), previousValues, i -> (int) hash.get(i));
    }

    private void assertLongHash(Set<Long> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(hash::add);
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeLong(Block.Ref.floating(b.block())).hash(hash);
        assertThat(hashes.sawNull(), equalTo(b.values().stream().anyMatch(v -> v == null)));
        assertHash(b, hashes.ords(), hash.size(), previousValues, i -> hash.get(i));
    }

    private void assertDoubleHash(Set<Double> previousValues, BasicBlockTests.RandomBlock b) {
        LongHash hash = new LongHash(1, BigArrays.NON_RECYCLING_INSTANCE);
        previousValues.stream().forEach(d -> hash.add(Double.doubleToLongBits(d)));
        MultivalueDedupe.HashResult hashes = new MultivalueDedupeDouble(Block.Ref.floating(b.block())).hash(hash);
        assertThat(hashes.sawNull(), equalTo(b.values().stream().anyMatch(v -> v == null)));
        assertHash(b, hashes.ords(), hash.size(), previousValues, i -> Double.longBitsToDouble(hash.get(i)));
    }

    private void assertHash(
        BasicBlockTests.RandomBlock b,
        IntBlock hashes,
        long hashSize,
        Set<? extends Object> previousValues,
        LongFunction<Object> lookup
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
            List<Object> actualValues = new ArrayList<>(count);
            int end = start + count;
            for (int i = start; i < end; i++) {
                actualValues.add(lookup.apply(hashes.getInt(i) - 1));
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
