/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class BlockBuilderTests extends ESTestCase {
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
            params.add(new Object[] { e });
        }
        return params;
    }

    private final ElementType elementType;

    BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));

    public BlockBuilderTests(ElementType elementType) {
        this.elementType = elementType;
    }

    public void testAllNulls() {
        for (int numEntries : List.of(1, between(1, 100), between(101, 1000))) {
            testAllNullsImpl(elementType.newBlockBuilder(0, blockFactory), numEntries);
            testAllNullsImpl(elementType.newBlockBuilder(numEntries, blockFactory), numEntries);
            testAllNullsImpl(elementType.newBlockBuilder(numEntries * 10, blockFactory), numEntries);
            testAllNullsImpl(elementType.newBlockBuilder(between(0, numEntries), blockFactory), numEntries);
        }
    }

    private void testAllNullsImpl(Block.Builder builder, int numEntries) {
        for (int i = 0; i < numEntries; i++) {
            builder.appendNull();
        }
        assertThat(
            builder.estimatedBytes(),
            both(greaterThan(blockFactory.breaker().getUsed() - 1024)).and(lessThan(blockFactory.breaker().getUsed() + 1024))
        );
        try (Block block = builder.build()) {
            assertThat(block.getPositionCount(), is(numEntries));
            for (int p = 0; p < numEntries; p++) {
                assertThat(block.isNull(p), is(true));
            }
            assertThat(block.areAllValuesNull(), is(true));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testCloseWithoutBuilding() {
        elementType.newBlockBuilder(10, blockFactory).close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testBuildSmallSingleValued() {
        testBuild(between(1, 100), false, 1);
    }

    public void testBuildHugeSingleValued() {
        testBuild(between(1_000, 50_000), false, 1);
    }

    public void testBuildSmallSingleValuedNullable() {
        testBuild(between(1, 100), true, 1);
    }

    public void testBuildHugeSingleValuedNullable() {
        testBuild(between(1_000, 50_000), true, 1);
    }

    public void testBuildSmallMultiValued() {
        testBuild(between(1, 100), false, 3);
    }

    public void testBuildHugeMultiValued() {
        testBuild(between(1_000, 50_000), false, 3);
    }

    public void testBuildSmallMultiValuedNullable() {
        testBuild(between(1, 100), true, 3);
    }

    public void testBuildHugeMultiValuedNullable() {
        testBuild(between(1_000, 50_000), true, 3);
    }

    public void testBuildSingle() {
        testBuild(1, false, 1);
    }

    private void testBuild(int size, boolean nullable, int maxValueCount) {
        try (Block.Builder builder = elementType.newBlockBuilder(randomBoolean() ? size : 1, blockFactory)) {
            RandomBlock random = RandomBlock.randomBlock(elementType, size, nullable, 1, maxValueCount, 0, 0);
            builder.copyFrom(random.block(), 0, random.block().getPositionCount());
            assertThat(
                builder.estimatedBytes(),
                both(greaterThan(blockFactory.breaker().getUsed() - 1024)).and(lessThan(blockFactory.breaker().getUsed() + 1024))
            );
            try (Block built = builder.build()) {
                assertThat(built, equalTo(random.block()));
                assertThat(blockFactory.breaker().getUsed(), equalTo(built.ramBytesUsed()));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testDoubleBuild() {
        try (Block.Builder builder = elementType.newBlockBuilder(10, blockFactory)) {
            RandomBlock random = RandomBlock.randomBlock(elementType, 10, false, 1, 1, 0, 0);
            builder.copyFrom(random.block(), 0, random.block().getPositionCount());
            try (Block built = builder.build()) {
                assertThat(built, equalTo(random.block()));
                assertThat(blockFactory.breaker().getUsed(), equalTo(built.ramBytesUsed()));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
            Exception e = expectThrows(IllegalStateException.class, builder::build);
            assertThat(e.getMessage(), equalTo("already closed"));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testCranky() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService());
        BlockFactory blockFactory = new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
        for (int i = 0; i < 100; i++) {
            try {
                try (Block.Builder builder = elementType.newBlockBuilder(10, blockFactory)) {
                    RandomBlock random = RandomBlock.randomBlock(elementType, 10, false, 1, 1, 0, 0);
                    builder.copyFrom(random.block(), 0, random.block().getPositionCount());
                    try (Block built = builder.build()) {
                        assertThat(built, equalTo(random.block()));
                    }
                }
                // If we made it this far cranky didn't fail us!
            } catch (CircuitBreakingException e) {
                logger.info("cranky", e);
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testCrankyConstantBlock() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new CrankyCircuitBreakerService());
        BlockFactory blockFactory = new BlockFactory(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST), bigArrays);
        for (int i = 0; i < 100; i++) {
            try {
                try (Block.Builder builder = elementType.newBlockBuilder(randomInt(10), blockFactory)) {
                    RandomBlock random = RandomBlock.randomBlock(elementType, 1, false, 1, 1, 0, 0);
                    builder.copyFrom(random.block(), 0, random.block().getPositionCount());
                    try (Block built = builder.build()) {
                        assertThat(built.asVector().isConstant(), is(true));
                        assertThat(built, equalTo(random.block()));
                    }
                }
                // If we made it this far cranky didn't fail us!
            } catch (CircuitBreakingException e) {
                logger.info("cranky", e);
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }
}
