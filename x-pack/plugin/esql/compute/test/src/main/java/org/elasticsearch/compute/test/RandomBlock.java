/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.ExponentialHistogramArrayBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlockBuilder;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.TDigestBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomMillisUpToYear9999;

/**
 * A block of random values.
 * @param values the values as java object
 * @param block randomly built block
 * @param valueMaxByteSize the maximum byte size of any single value
 */
public record RandomBlock(List<List<Object>> values, Block block, int valueMaxByteSize) {
    /**
     * A random {@link ElementType} for which we can build a {@link RandomBlock}.
     */
    public static ElementType randomElementType() {
        return randomElementExcluding(List.of());
    }

    public static ElementType randomElementExcluding(List<ElementType> type) {
        return ESTestCase.randomValueOtherThanMany(
            e -> e == ElementType.UNKNOWN
                || e == ElementType.NULL
                || e == ElementType.DOC
                || e == ElementType.COMPOSITE
                || e == ElementType.LONG_RANGE
                || type.contains(e),
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
        if (elementType == ElementType.EXPONENTIAL_HISTOGRAM || elementType == ElementType.TDIGEST) {
            // histograms do not support multi-values
            // TODO(b/133393) remove this when we support multi-values in exponential histogram blocks
            minValuesPerPosition = Math.min(1, minValuesPerPosition);
            maxValuesPerPosition = Math.min(1, maxValuesPerPosition);
            minDupsPerPosition = 0;
            maxDupsPerPosition = 0;
            mvOrdering = Block.MvOrdering.UNORDERED; // histograms do not support ordering
        }
        try (var builder = elementType.newBlockBuilder(positionCount, blockFactory)) {
            boolean bytesRefFromPoints = ESTestCase.randomBoolean();
            Supplier<Point> pointSupplier = ESTestCase.randomBoolean() ? GeometryTestUtils::randomPoint : ShapeTestUtils::randomPoint;
            int valueMaxByteSize = switch (elementType) {
                case BOOLEAN -> Byte.BYTES;
                case INT -> Integer.BYTES;
                case LONG -> Long.BYTES;
                case FLOAT -> Float.BYTES;
                case DOUBLE -> Double.BYTES;
                case NULL -> 0;
                case DOC -> 3 * Integer.BYTES;
                case LONG_RANGE -> 2 * Long.BYTES;
                case BYTES_REF, EXPONENTIAL_HISTOGRAM -> 0; // Updated per value below
                case AGGREGATE_METRIC_DOUBLE -> 3 * Double.BYTES + Integer.BYTES;
                case TDIGEST -> 0; // TDIGEST has no well-defined single-value byte size
                case COMPOSITE, UNKNOWN -> throw new IllegalArgumentException("can't build a random " + elementType + " block");
            };
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
                                ? new BytesRef(WellKnownBinary.toWKB(pointSupplier.get(), ByteOrder.LITTLE_ENDIAN))
                                : new BytesRef(ESTestCase.randomRealisticUnicodeOfLength(4));
                            valuesAtPosition.add(b);
                            ((BytesRefBlock.Builder) builder).appendBytesRef(b);
                            valueMaxByteSize = Math.max(valueMaxByteSize, b.length);
                        }
                        case BOOLEAN -> {
                            boolean b = ESTestCase.randomBoolean();
                            valuesAtPosition.add(b);
                            ((BooleanBlock.Builder) builder).appendBoolean(b);
                        }
                        case AGGREGATE_METRIC_DOUBLE -> {
                            AggregateMetricDoubleBlockBuilder b = (AggregateMetricDoubleBlockBuilder) builder;
                            double min = ESTestCase.randomDouble();
                            double max = ESTestCase.randomDouble();
                            double sum = ESTestCase.randomDouble();
                            int count = ESTestCase.randomNonNegativeInt();
                            b.min().appendDouble(min);
                            b.max().appendDouble(max);
                            b.sum().appendDouble(sum);
                            b.count().appendInt(count);
                            valuesAtPosition.add(new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(min, max, sum, count));
                        }
                        case EXPONENTIAL_HISTOGRAM -> {
                            ExponentialHistogramBlockBuilder b = (ExponentialHistogramBlockBuilder) builder;
                            ExponentialHistogram histogram = BlockTestUtils.randomExponentialHistogram();
                            b.append(histogram);
                            valuesAtPosition.add(histogram);
                            valueMaxByteSize = Math.max(
                                valueMaxByteSize,
                                5 * Double.BYTES + ExponentialHistogramArrayBlock.encode(histogram).encodedHistogram().length
                            );
                        }
                        case TDIGEST -> {
                            TDigestBlockBuilder b = (TDigestBlockBuilder) builder;
                            TDigestHolder digest = BlockTestUtils.randomTDigest();
                            b.appendTDigest(digest);
                            valuesAtPosition.add(digest);
                            valueMaxByteSize = Math.max(valueMaxByteSize, 3 * Double.BYTES + Long.BYTES + digest.getEncodedDigest().length);
                        }
                        case LONG_RANGE -> {
                            var b = (LongRangeBlockBuilder) builder;
                            var from = randomMillisUpToYear9999();
                            var to = randomLongBetween(from + 1, MAX_MILLIS_BEFORE_9999);
                            b.from().appendLong(from);
                            b.to().appendLong(to);
                            valuesAtPosition.add(new LongRangeBlockBuilder.LongRange(from, to));
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
            Block builtBlock = builder.build();
            return new RandomBlock(values, builtBlock, valueMaxByteSize);
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
            Block builtBlock = mergedBlock.build();
            return new RandomBlock(mergedValues, builtBlock, Math.max(valueMaxByteSize, rhs.valueMaxByteSize));
        }
    }

}
