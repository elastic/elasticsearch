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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.function.ToDoubleFunction;

/**
 * Conversion of TDigestBlocks to an Arrow struct
 * {@code Struct(min: float64, max: float64, sum: float64, centroids: List<float64>, counts: List<int64>)}.
 * <p>
 * The struct is null for a missing digest. An empty digest is a valid struct with empty lists. {@code min}, {@code max}
 * and {@code sum} are null when absent (which is the case for empty digests), following the Arrow convention that NaN
 * is a value and missing data is null. The lists are never null. The number of samples in the digest is not a field,
 * as it is the sum of {@code counts}.
 * <p>
 * T-Digests are always single-valued, so columns of this type are never encoded as Arrow lists.
 */
class TDigestArrowFormatter extends BlockArrowFormatter {
    private final DataType esqlType;

    TDigestArrowFormatter(DataType esqlType) {
        super(esqlType, Types.MinorType.STRUCT);
        this.esqlType = esqlType;
    }

    @Override
    public Field arrowField(String name) {
        var doubleType = FieldType.nullable(Types.MinorType.FLOAT8.getType());
        return new Field(
            name,
            super.arrowField(name).getFieldType(),
            List.of(
                new Field("min", doubleType, null),
                new Field("max", doubleType, null),
                new Field("sum", doubleType, null),
                nonNullableListField("centroids", Types.MinorType.FLOAT8),
                nonNullableListField("counts", Types.MinorType.BIGINT)
            )
        );
    }

    @Override
    public boolean isMultivalued(String columnName, Block block) {
        return forbidMultivalued(columnName, esqlType, block);
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
        assert asList == false : "tdigest columns cannot be encoded as Arrow lists";
        TDigestBlock block = (TDigestBlock) b;
        int positions = block.getPositionCount();
        int[] offsets = centroidOffsets(block);
        int centroids = offsets[positions];
        int minAbsent = absentCount(block, TDigestHolder::getMin);
        int maxAbsent = absentCount(block, TDigestHolder::getMax);
        int sumAbsent = absentCount(block, TDigestHolder::getSum);

        nodes.add(new ArrowFieldNode(positions, block.nullValuesCount())); // struct
        nodes.add(new ArrowFieldNode(positions, minAbsent)); // min
        nodes.add(new ArrowFieldNode(positions, maxAbsent)); // max
        nodes.add(new ArrowFieldNode(positions, sumAbsent)); // sum
        nodes.add(new ArrowFieldNode(positions, 0)); // centroids list
        nodes.add(new ArrowFieldNode(centroids, 0)); // centroids values
        nodes.add(new ArrowFieldNode(positions, 0)); // counts list
        nodes.add(new ArrowFieldNode(centroids, 0)); // counts values

        // Struct validity
        accumulateVectorValidity(bufs, bufWriters, block, false);

        addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMin, minAbsent);
        addDoubleChild(bufs, bufWriters, block, TDigestHolder::getMax, maxAbsent);
        addDoubleChild(bufs, bufWriters, block, TDigestHolder::getSum, sumAbsent);

        addListChild(bufs, bufWriters, block, offsets, Double.BYTES, (out, it) -> out.writeDoubleLE(it.currentMean()));
        addListChild(bufs, bufWriters, block, offsets, Long.BYTES, (out, it) -> out.writeLongLE(it.currentCount()));
    }

    /**
     * Offsets of each position's centroids in the list values, {@code positionCount + 1} entries. Null digests have no centroids.
     */
    private static int[] centroidOffsets(TDigestBlock block) {
        int positions = block.getPositionCount();
        int[] offsets = new int[positions + 1];
        TDigestHolder scratch = new TDigestHolder();
        long total = 0;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position) == false) {
                total += block.getTDigestHolder(block.getFirstValueIndex(position), scratch).centroidCount();
            }
            offsets[position + 1] = Math.toIntExact(total);
        }
        return offsets;
    }

    /** The value of a float64 child at a position, {@code NaN} if the digest is null or doesn't have that value. */
    private static double childValue(TDigestBlock block, int position, ToDoubleFunction<TDigestHolder> getter, TDigestHolder scratch) {
        if (block.isNull(position)) {
            return Double.NaN;
        }
        return getter.applyAsDouble(block.getTDigestHolder(block.getFirstValueIndex(position), scratch));
    }

    private static int absentCount(TDigestBlock block, ToDoubleFunction<TDigestHolder> getter) {
        TDigestHolder scratch = new TDigestHolder();
        int count = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (Double.isNaN(childValue(block, position, getter, scratch))) {
                count++;
            }
        }
        return count;
    }

    /** A nullable float64 child: absent values are null, with a zero placeholder in the values buffer. */
    private static void addDoubleChild(
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        TDigestBlock block,
        ToDoubleFunction<TDigestHolder> getter,
        int absentCount
    ) {
        int positions = block.getPositionCount();
        if (absentCount == 0) {
            addEmptyValidity(bufs, bufWriters);
        } else {
            bufs.add(dummyArrowBuf(bitSetLength(positions)));
            bufWriters.add(out -> {
                TDigestHolder scratch = new TDigestHolder();
                BitSet bits = new BitSet(positions);
                for (int position = 0; position < positions; position++) {
                    if (Double.isNaN(childValue(block, position, getter, scratch)) == false) {
                        bits.set(position);
                    }
                }
                return writeBitSet(out, bits, positions);
            });
        }
        bufs.add(dummyArrowBuf((long) positions * Double.BYTES));
        bufWriters.add(out -> {
            TDigestHolder scratch = new TDigestHolder();
            for (int position = 0; position < positions; position++) {
                double value = childValue(block, position, getter, scratch);
                out.writeDoubleLE(Double.isNaN(value) ? 0 : value);
            }
            return (long) positions * Double.BYTES;
        });
    }

    @FunctionalInterface
    private interface CentroidWriter {
        void write(RecyclerBytesStreamOutput out, EncodedTDigest.CentroidIterator centroid) throws IOException;
    }

    /** A non-nullable list child: no validity buffers (null digests are empty lists), offsets, then the values. */
    private static void addListChild(
        List<ArrowBuf> bufs,
        List<BufWriter> bufWriters,
        TDigestBlock block,
        int[] offsets,
        int itemBytes,
        CentroidWriter itemWriter
    ) {
        int positions = block.getPositionCount();
        addNonNullableListBuffers(bufs, bufWriters, offsets, itemBytes, out -> {
            TDigestHolder scratch = new TDigestHolder();
            EncodedTDigest decoder = new EncodedTDigest();
            long bytes = 0;
            for (int position = 0; position < positions; position++) {
                if (block.isNull(position)) {
                    continue;
                }
                BytesRef encoded = block.getTDigestHolder(block.getFirstValueIndex(position), scratch).getEncodedDigest();
                if (encoded.length == 0) {
                    continue;
                }
                decoder.reset(encoded);
                var it = decoder.centroidIterator();
                while (it.next()) {
                    itemWriter.write(out, it);
                    bytes += itemBytes;
                }
            }
            return bytes;
        });
    }
}
