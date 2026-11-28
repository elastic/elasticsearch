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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

/**
 * Classic histograms store bucket upper-limit values and counts using the {@link EncodedTDigest} binary format.
 * Preserve their values and counts as two parallel Arrow lists; an empty histogram remains a valid struct with empty
 * lists, distinct from a null histogram.
 */
class HistogramArrowFormatter extends BlockArrowFormatter {
    HistogramArrowFormatter(DataType esqlType) {
        super(esqlType, Types.MinorType.STRUCT);
    }

    @Override
    public Field arrowField(String name) {
        return new Field(
            name,
            super.arrowField(name).getFieldType(),
            List.of(nonNullableListField("values", Types.MinorType.FLOAT8), nonNullableListField("counts", Types.MinorType.BIGINT))
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
        BytesRefBlock block = (BytesRefBlock) b;
        int[] indexes = itemIndexes(block, asList);
        int items = indexes.length;
        int[] offsets = offsets(block, indexes);
        int values = offsets[items];
        addSimpleFieldNodes(block, asList, nodes);
        if (asList) {
            addListOffsets(bufs, bufWriters, block);
        }
        nodes.add(new ArrowFieldNode(items, 0));
        nodes.add(new ArrowFieldNode(values, 0));
        nodes.add(new ArrowFieldNode(items, 0));
        nodes.add(new ArrowFieldNode(values, 0));
        accumulateVectorValidity(bufs, bufWriters, block, asList);
        addNonNullableListBuffers(bufs, bufWriters, offsets, Double.BYTES, out -> writeBuckets(block, indexes, out, true));
        addNonNullableListBuffers(bufs, bufWriters, offsets, Long.BYTES, out -> writeBuckets(block, indexes, out, false));
    }

    private static int[] offsets(BytesRefBlock block, int[] indexes) {
        int[] offsets = new int[indexes.length + 1];
        EncodedTDigest decoder = new EncodedTDigest();
        BytesRef scratch = new BytesRef();
        long count = 0;
        for (int item = 0; item < indexes.length; item++) {
            if (indexes[item] >= 0) {
                decoder.reset(block.getBytesRef(indexes[item], scratch));
                count += decoder.centroidCount();
            }
            offsets[item + 1] = Math.toIntExact(count);
        }
        return offsets;
    }

    private static long writeBuckets(BytesRefBlock block, int[] indexes, RecyclerBytesStreamOutput out, boolean values) throws IOException {
        EncodedTDigest decoder = new EncodedTDigest();
        BytesRef scratch = new BytesRef();
        long bytes = 0;
        for (int index : indexes) {
            if (index < 0) {
                continue;
            }
            decoder.reset(block.getBytesRef(index, scratch));
            var it = decoder.centroidIterator();
            while (it.next()) {
                if (values) {
                    out.writeDoubleLE(it.currentMean());
                    bytes += Double.BYTES;
                } else {
                    out.writeLongLE(it.currentCount());
                    bytes += Long.BYTES;
                }
            }
        }
        return bytes;
    }
}
