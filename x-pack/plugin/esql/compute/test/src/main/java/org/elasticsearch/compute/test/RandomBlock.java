/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A block of random values.
 * @param values the values as java object
 * @param block randomly built block
 */
public record RandomBlock(List<List<Object>> values, Block block) {
    /**
     * A random {@link ElementType} for which we can build a {@link RandomBlock}.
     */
    public static ElementType randomElementType() {
        return ESTestCase.randomValueOtherThanMany(
            e -> e == ElementType.UNKNOWN
                || e == ElementType.NULL
                || e == ElementType.DOC
                || e == ElementType.COMPOSITE
                || e == ElementType.AGGREGATE_METRIC_DOUBLE,
            () -> ESTestCase.randomFrom(ElementType.values())
        );
    }

    // TODO Some kind of builder for this with nice defaults. We do this all the time and there's like a zillion parameters.

    public static RandomBlock randomBlock(
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        return randomBlock(
            TestBlockFactory.getNonBreakingInstance(),
            elementType,
            positionCount,
            nullAllowed,
            minValuesPerPosition,
            maxValuesPerPosition,
            minDupsPerPosition,
            maxDupsPerPosition
        );
    }

    public static RandomBlock randomBlock(
        BlockFactory blockFactory,
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        List<List<Object>> values = new ArrayList<>();
        Block.MvOrdering mvOrdering = Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
        try (var builder = elementType.newBlockBuilder(positionCount, blockFactory)) {
            boolean bytesRefFromPoints = ESTestCase.randomBoolean();
            Supplier<Point> pointSupplier = ESTestCase.randomBoolean() ? GeometryTestUtils::randomPoint : ShapeTestUtils::randomPoint;
            for (int p = 0; p < positionCount; p++) {
                if (elementType == ElementType.NULL) {
                    assert nullAllowed;
                    values.add(null);
                    builder.appendNull();
                    continue;
                }
                int valueCount = ESTestCase.between(minValuesPerPosition, maxValuesPerPosition);
                if (valueCount == 0 || nullAllowed && ESTestCase.randomBoolean()) {
                    values.add(null);
                    builder.appendNull();
                    continue;
                }
                int dupCount = ESTestCase.between(minDupsPerPosition, maxDupsPerPosition);
                if (valueCount != 1 || dupCount != 0) {
                    builder.beginPositionEntry();
                }
                List<Object> valuesAtPosition = new ArrayList<>();
                values.add(valuesAtPosition);
                for (int v = 0; v < valueCount; v++) {
                    switch (elementType) {
                        case INT -> {
                            int i = ESTestCase.randomInt();
                            valuesAtPosition.add(i);
                            ((IntBlock.Builder) builder).appendInt(i);
                        }
                        case LONG -> {
                            long l = ESTestCase.randomLong();
                            valuesAtPosition.add(l);
                            ((LongBlock.Builder) builder).appendLong(l);
                        }
                        case FLOAT -> {
                            float f = ESTestCase.randomFloat();
                            valuesAtPosition.add(f);
                            ((FloatBlock.Builder) builder).appendFloat(f);
                        }
                        case DOUBLE -> {
                            double d = ESTestCase.randomDouble();
                            valuesAtPosition.add(d);
                            ((DoubleBlock.Builder) builder).appendDouble(d);
                        }
                        case BYTES_REF -> {
                            BytesRef b = bytesRefFromPoints
                                ? SpatialCoordinateTypes.GEO.asWkb(pointSupplier.get())
                                : new BytesRef(ESTestCase.randomRealisticUnicodeOfLength(4));
                            valuesAtPosition.add(b);
                            ((BytesRefBlock.Builder) builder).appendBytesRef(b);
                        }
                        case BOOLEAN -> {
                            boolean b = ESTestCase.randomBoolean();
                            valuesAtPosition.add(b);
                            ((BooleanBlock.Builder) builder).appendBoolean(b);
                        }
                        default -> throw new IllegalArgumentException("unsupported element type [" + elementType + "]");
                    }
                }
                for (int i = 0; i < dupCount; i++) {
                    BlockTestUtils.append(builder, ESTestCase.randomFrom(valuesAtPosition));
                }
                if (valueCount != 1 || dupCount != 0) {
                    builder.endPositionEntry();
                }
                if (dupCount > 0) {
                    mvOrdering = Block.MvOrdering.UNORDERED;
                } else if (mvOrdering != Block.MvOrdering.UNORDERED) {
                    List<Object> dedupedAndSortedList = valuesAtPosition.stream().sorted().distinct().toList();
                    if (dedupedAndSortedList.size() != valuesAtPosition.size()) {
                        mvOrdering = Block.MvOrdering.UNORDERED;
                    } else if (dedupedAndSortedList.equals(valuesAtPosition) == false) {
                        mvOrdering = Block.MvOrdering.DEDUPLICATED_UNORDERD;
                    }
                }
            }
            if (ESTestCase.randomBoolean()) {
                builder.mvOrdering(mvOrdering);
            }
            return new RandomBlock(values, builder.build());
        }
    }

    public int valueCount() {
        return values.stream().mapToInt(l -> l == null ? 0 : l.size()).sum();
    }

    /**
     * Build a {@link RandomBlock} contain the values of two blocks, preserving the relative order.
     */
    public RandomBlock merge(RandomBlock rhs) {
        int estimatedSize = values().size() + rhs.values().size();
        int l = 0;
        int r = 0;
        List<List<Object>> mergedValues = new ArrayList<>(estimatedSize);
        try (Block.Builder mergedBlock = block.elementType().newBlockBuilder(estimatedSize, block.blockFactory())) {
            while (l < values.size() && r < rhs.values.size()) {
                if (ESTestCase.randomBoolean()) {
                    mergedValues.add(values.get(l));
                    mergedBlock.copyFrom(block, l, l + 1);
                    l++;
                } else {
                    mergedValues.add(rhs.values.get(r));
                    mergedBlock.copyFrom(rhs.block, r, r + 1);
                    r++;
                }
            }
            while (l < values.size()) {
                mergedValues.add(values.get(l));
                mergedBlock.copyFrom(block, l, l + 1);
                l++;
            }
            while (r < rhs.values.size()) {
                mergedValues.add(rhs.values.get(r));
                mergedBlock.copyFrom(rhs.block, r, r + 1);
                r++;
            }
            return new RandomBlock(mergedValues, mergedBlock.build());
        }
    }
}
