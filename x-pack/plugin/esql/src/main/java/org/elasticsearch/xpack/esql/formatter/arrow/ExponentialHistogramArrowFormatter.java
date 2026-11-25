/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramScratch;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

/**
 * Conversion of {@link ExponentialHistogramBlock} to an Arrow struct matching the {@code exponential_histogram} field
 * structure: {@code Struct(scale: int32, sum: float64, min: float64, max: float64, zero: Struct(count: int64,
 * threshold: float64), positive: Struct(indices: List<int64>, counts: List<int64>), negative: Struct(indices:
 * List<int64>, counts: List<int64>))}.
 * <p>
 * The struct is null for a missing histogram. An empty histogram is a valid struct with empty bucket lists and null
 * {@code sum}, {@code min}, and {@code max} (following the Arrow convention that missing values are null rather than
 * NaN). The {@code scale}, {@code zero}, {@code positive}, and {@code negative} children and their nested fields and
 * lists are non-nullable. Total sample count is omitted as it is {@code zero.count + sum(positive.counts) +
 * sum(negative.counts)}. Multivalued columns are encoded as a list of structs.
 */
class ExponentialHistogramArrowFormatter extends BlockArrowFormatter {
    ExponentialHistogramArrowFormatter(DataType esqlType) {
        super(esqlType, Types.MinorType.STRUCT);
    }

    @Override
    public Field arrowField(String name) {
        var intType = new FieldType(false, Types.MinorType.INT.getType(), null);
        var longType = new FieldType(false, Types.MinorType.BIGINT.getType(), null);
        var doubleType = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
        var nullableDouble = FieldType.nullable(Types.MinorType.FLOAT8.getType());
        var zero = new Field(
            "zero",
            new FieldType(false, Types.MinorType.STRUCT.getType(), null),
            List.of(new Field("count", longType, null), new Field("threshold", doubleType, null))
        );
        var buckets = List.of(
            nonNullableListField("indices", Types.MinorType.BIGINT),
            nonNullableListField("counts", Types.MinorType.BIGINT)
        );
        var structType = new FieldType(false, Types.MinorType.STRUCT.getType(), null);
        return new Field(
            name,
            super.arrowField(name).getFieldType(),
            List.of(
                new Field("scale", intType, null),
                new Field("sum", nullableDouble, null),
                new Field("min", nullableDouble, null),
                new Field("max", nullableDouble, null),
                zero,
                new Field("positive", structType, buckets),
                new Field("negative", structType, buckets)
            )
        );
    }

    @Override
    public void convert(
        Block b,
        boolean asList,
        List<ArrowFieldNode> nodes,
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        List<Releasable> releasables
    ) {
        ExponentialHistogramBlock block = (ExponentialHistogramBlock) b;
        int[] indexes = itemIndexes(block, asList);
        int items = indexes.length;

        int[] scales = new int[items];
        long[] zeroCounts = new long[items];
        double[] zeroThresholds = new double[items];
        int[] positiveOffsets = new int[items + 1];
        int[] negativeOffsets = new int[items + 1];
        LongVector positiveIndices;
        LongVector positiveCounts;
        LongVector negativeIndices;
        LongVector negativeCounts;
        try (
            LongVector.Builder positiveIndicesBuilder = block.blockFactory().newLongVectorBuilder(items);
            LongVector.Builder positiveCountsBuilder = block.blockFactory().newLongVectorBuilder(items);
            LongVector.Builder negativeIndicesBuilder = block.blockFactory().newLongVectorBuilder(items);
            LongVector.Builder negativeCountsBuilder = block.blockFactory().newLongVectorBuilder(items)
        ) {
            ExponentialHistogramScratch scratch = new ExponentialHistogramScratch();
            int positiveTotal = 0;
            int negativeTotal = 0;
            for (int item = 0; item < items; item++) {
                int index = indexes[item];
                if (index >= 0) {
                    var histogram = block.getExponentialHistogram(index, scratch);
                    scales[item] = histogram.scale();
                    var zero = histogram.zeroBucket();
                    zeroCounts[item] = zero.count();
                    double threshold = zero.zeroThreshold();
                    zeroThresholds[item] = Double.isNaN(threshold) ? 0 : threshold;
                    positiveTotal = appendBuckets(
                        histogram.positiveBuckets().iterator(),
                        positiveIndicesBuilder,
                        positiveCountsBuilder,
                        positiveTotal
                    );
                    negativeTotal = appendBuckets(
                        histogram.negativeBuckets().iterator(),
                        negativeIndicesBuilder,
                        negativeCountsBuilder,
                        negativeTotal
                    );
                }
                positiveOffsets[item + 1] = positiveTotal;
                negativeOffsets[item + 1] = negativeTotal;
            }
            positiveIndices = positiveIndicesBuilder.build();
            releasables.add(positiveIndices);
            positiveCounts = positiveCountsBuilder.build();
            releasables.add(positiveCounts);
            negativeIndices = negativeIndicesBuilder.build();
            releasables.add(negativeIndices);
            negativeCounts = negativeCountsBuilder.build();
            releasables.add(negativeCounts);
        }

        DoubleBlock sumBlock = block.getSumBlock();
        DoubleBlock minBlock = block.getMinBlock();
        DoubleBlock maxBlock = block.getMaxBlock();
        int sumAbsent = absentCount(sumBlock, indexes);
        int minAbsent = absentCount(minBlock, indexes);
        int maxAbsent = absentCount(maxBlock, indexes);

        addSimpleFieldNodes(block, asList, nodes);
        if (asList) {
            addListOffsets(bufs, bufWriters, block);
        }
        accumulateVectorValidity(bufs, bufWriters, block, asList);

        nodes.add(new ArrowFieldNode(items, 0)); // scale
        addIntChild(bufs, bufWriters, scales);

        nodes.add(new ArrowFieldNode(items, sumAbsent)); // sum
        addDoubleSubBlockChild(bufs, bufWriters, sumBlock, indexes, sumAbsent);

        nodes.add(new ArrowFieldNode(items, minAbsent)); // min
        addDoubleSubBlockChild(bufs, bufWriters, minBlock, indexes, minAbsent);

        nodes.add(new ArrowFieldNode(items, maxAbsent)); // max
        addDoubleSubBlockChild(bufs, bufWriters, maxBlock, indexes, maxAbsent);

        nodes.add(new ArrowFieldNode(items, 0)); // zero
        nodes.add(new ArrowFieldNode(items, 0)); // zero.count
        nodes.add(new ArrowFieldNode(items, 0)); // zero.threshold
        addEmptyValidity(bufs, bufWriters); // zero struct
        addLongChild(bufs, bufWriters, zeroCounts);
        addDoubleChild(bufs, bufWriters, zeroThresholds);

        addBucketNodes(nodes, items, positiveIndices.getPositionCount());
        addBucketBuffers(bufs, bufWriters, positiveOffsets, positiveIndices, positiveCounts);

        addBucketNodes(nodes, items, negativeIndices.getPositionCount());
        addBucketBuffers(bufs, bufWriters, negativeOffsets, negativeIndices, negativeCounts);
    }

    private static int appendBuckets(BucketIterator buckets, LongVector.Builder indices, LongVector.Builder counts, int total) {
        while (buckets.hasNext()) {
            indices.appendLong(buckets.peekIndex());
            counts.appendLong(buckets.peekCount());
            total = Math.addExact(total, 1);
            buckets.advance();
        }
        return total;
    }

    private static void addBucketNodes(List<ArrowFieldNode> nodes, int positions, int buckets) {
        nodes.add(new ArrowFieldNode(positions, 0)); // bucket struct
        nodes.add(new ArrowFieldNode(positions, 0)); // indices list
        nodes.add(new ArrowFieldNode(buckets, 0)); // indices
        nodes.add(new ArrowFieldNode(positions, 0)); // counts list
        nodes.add(new ArrowFieldNode(buckets, 0)); // counts
    }

    private static void addBucketBuffers(
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        int[] offsets,
        LongVector indices,
        LongVector counts
    ) {
        addEmptyValidity(bufs, bufWriters); // bucket struct
        addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeLongs(out, indices));
        addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeLongs(out, counts));
    }

    private static int absentCount(DoubleBlock subBlock, int[] indexes) {
        if (subBlock.mayHaveNulls() == false) {
            return 0;
        }
        if (subBlock.areAllValuesNull()) {
            return indexes.length;
        }
        int count = 0;
        for (int index : indexes) {
            if (index < 0 || subBlock.isNull(index)) {
                count++;
            }
        }
        return count;
    }

    private static void addDoubleSubBlockChild(
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        DoubleBlock subBlock,
        int[] indexes,
        int absentCount
    ) {
        int items = indexes.length;
        if (absentCount == 0) {
            addEmptyValidity(bufs, bufWriters);
        } else {
            bufs.add(dummyArrowBuf(bitSetLength(items)));
            bufWriters.add(out -> {
                if (subBlock.areAllValuesNull()) {
                    return writeAllFalseValidity(out, items);
                }
                BitSet valid = new BitSet(items);
                for (int item = 0; item < items; item++) {
                    int index = indexes[item];
                    if (index >= 0 && subBlock.isNull(index) == false) {
                        valid.set(item);
                    }
                }
                return writeBitSet(out, valid, items);
            });
        }
        long bytes = (long) items * Double.BYTES;
        bufs.add(dummyArrowBuf(bytes));
        bufWriters.add(out -> {
            if (subBlock.areAllValuesNull()) {
                return writeZeroes(out, bytes);
            }
            for (int index : indexes) {
                if (index < 0 || subBlock.isNull(index)) {
                    out.writeDoubleLE(0);
                } else {
                    out.writeDoubleLE(subBlock.getDouble(subBlock.getFirstValueIndex(index)));
                }
            }
            return bytes;
        });
    }

    private static void addIntChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, int[] values) {
        addEmptyValidity(bufs, bufWriters);
        long bytes = (long) values.length * Integer.BYTES;
        bufs.add(dummyArrowBuf(bytes));
        bufWriters.add(out -> {
            for (int value : values) {
                out.writeIntLE(value);
            }
            return bytes;
        });
    }

    private static void addLongChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, long[] values) {
        addEmptyValidity(bufs, bufWriters);
        long bytes = (long) values.length * Long.BYTES;
        bufs.add(dummyArrowBuf(bytes));
        bufWriters.add(out -> {
            for (long value : values) {
                out.writeLongLE(value);
            }
            return bytes;
        });
    }

    private static void addDoubleChild(List<ArrowBuf> bufs, List<BufWriter> bufWriters, double[] values) {
        addEmptyValidity(bufs, bufWriters);
        long bytes = (long) values.length * Double.BYTES;
        bufs.add(dummyArrowBuf(bytes));
        bufWriters.add(out -> {
            for (double value : values) {
                out.writeDoubleLE(value);
            }
            return bytes;
        });
    }

    private static long writeLongs(RecyclerBytesStreamOutput out, LongVector values) throws IOException {
        int count = values.getPositionCount();
        for (int i = 0; i < count; i++) {
            out.writeLongLE(values.getLong(i));
        }
        return (long) count * Long.BYTES;
    }
}
