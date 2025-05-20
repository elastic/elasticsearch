/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
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

import static org.hamcrest.Matchers.equalTo;

public class VectorBuilderTests extends ESTestCase {
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

    public VectorBuilderTests(ElementType elementType) {
        this.elementType = elementType;
    }

    public void testCloseWithoutBuilding() {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));
        vectorBuilder(10, blockFactory).close();
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testBuildSmall() {
        testBuild(between(1, 100));
    }

    public void testBuildHuge() {
        testBuild(between(1_000, 50_000));
    }

    public void testBuildSingle() {
        testBuild(1);
    }

    private void testBuild(int size) {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));
        try (Vector.Builder builder = vectorBuilder(randomBoolean() ? size : 1, blockFactory)) {
            RandomBlock random = RandomBlock.randomBlock(elementType, size, false, 1, 1, 0, 0);
            fill(builder, random.block().asVector());
            try (Vector built = builder.build()) {
                assertThat(built, equalTo(random.block().asVector()));
                assertThat(blockFactory.breaker().getUsed(), equalTo(built.ramBytesUsed()));
            }
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testDoubleBuild() {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofGb(1));
        try (Vector.Builder builder = vectorBuilder(10, blockFactory)) {
            RandomBlock random = RandomBlock.randomBlock(elementType, 10, false, 1, 1, 0, 0);
            fill(builder, random.block().asVector());
            try (Vector built = builder.build()) {
                assertThat(built, equalTo(random.block().asVector()));
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
                try (Vector.Builder builder = vectorBuilder(10, blockFactory)) {
                    RandomBlock random = RandomBlock.randomBlock(elementType, 10, false, 1, 1, 0, 0);
                    fill(builder, random.block().asVector());
                    try (Vector built = builder.build()) {
                        assertThat(built, equalTo(random.block().asVector()));
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

    private Vector.Builder vectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        return switch (elementType) {
            case NULL, DOC, COMPOSITE, AGGREGATE_METRIC_DOUBLE, UNKNOWN -> throw new UnsupportedOperationException();
            case BOOLEAN -> blockFactory.newBooleanVectorBuilder(estimatedSize);
            case BYTES_REF -> blockFactory.newBytesRefVectorBuilder(estimatedSize);
            case FLOAT -> blockFactory.newFloatVectorBuilder(estimatedSize);
            case DOUBLE -> blockFactory.newDoubleVectorBuilder(estimatedSize);
            case INT -> blockFactory.newIntVectorBuilder(estimatedSize);
            case LONG -> blockFactory.newLongVectorBuilder(estimatedSize);
        };
    }

    private void fill(Vector.Builder builder, Vector from) {
        switch (elementType) {
            case NULL, DOC, COMPOSITE, AGGREGATE_METRIC_DOUBLE, UNKNOWN -> throw new UnsupportedOperationException();
            case BOOLEAN -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((BooleanVector.Builder) builder).appendBoolean(((BooleanVector) from).getBoolean(p));
                }
            }
            case BYTES_REF -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((BytesRefVector.Builder) builder).appendBytesRef(((BytesRefVector) from).getBytesRef(p, new BytesRef()));
                }
            }
            case FLOAT -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((FloatVector.Builder) builder).appendFloat(((FloatVector) from).getFloat(p));
                }
            }
            case DOUBLE -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((DoubleVector.Builder) builder).appendDouble(((DoubleVector) from).getDouble(p));
                }
            }
            case INT -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((IntVector.Builder) builder).appendInt(((IntVector) from).getInt(p));
                }
            }
            case LONG -> {
                for (int p = 0; p < from.getPositionCount(); p++) {
                    ((LongVector.Builder) builder).appendLong(((LongVector) from).getLong(p));
                }
            }
        }
    }
}
