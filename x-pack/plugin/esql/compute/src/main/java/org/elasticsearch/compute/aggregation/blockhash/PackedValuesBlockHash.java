/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BatchEncoder;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.MultivalueDedupe;

import java.util.Arrays;
import java.util.List;

/**
 * Maps any number of columns to a group ids with every unique combination resulting
 * in a unique group id. Works by uniqing the values of each column and concatenating
 * the combinatorial explosion of all values into a byte array and then hashing each
 * byte array. If the values are
 * <pre>{@code
 *     a=(1, 2, 3) b=(2, 3) c=(4, 5, 5)
 * }</pre>
 * Then you get these grouping keys:
 * <pre>{@code
 *     1, 2, 4
 *     1, 2, 5
 *     1, 3, 4
 *     1, 3, 5
 *     2, 2, 4
 *     2, 2, 5
 *     2, 3, 4
 *     2, 3, 5
 *     3, 2, 4
 *     3, 3, 5
 * }</pre>
 */
final class PackedValuesBlockHash extends BlockHash {
    static final int DEFAULT_BATCH_SIZE = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());

    private final List<HashAggregationOperator.GroupSpec> groups;
    private final int emitBatchSize;
    private final BytesRefHash bytesRefHash;
    private final int nullTrackingBytes;

    PackedValuesBlockHash(List<HashAggregationOperator.GroupSpec> groups, BigArrays bigArrays, int emitBatchSize) {
        this.groups = groups;
        this.emitBatchSize = emitBatchSize;
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
        this.nullTrackingBytes = groups.size() / 8 + 1;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        add(page, addInput, DEFAULT_BATCH_SIZE);
    }

    void add(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
        new AddWork(page, addInput, batchSize).add();
    }

    class AddWork extends LongLongBlockHash.AbstractAddBlock {
        final BatchEncoder[] encoders = new BatchEncoder[groups.size()];
        final int[] positionOffsets = new int[groups.size()];
        final int[] valueOffsets = new int[groups.size()];
        final BytesRef scratch = new BytesRef();
        final int[] loopedIndices = new int[groups.size()];
        final int[] valueCounts = new int[groups.size()];
        final int[] bytesStarts = new int[groups.size()];
        final BytesRefBuilder bytes = new BytesRefBuilder();
        final int positionCount;

        int position;

        AddWork(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            super(emitBatchSize, addInput);
            for (int g = 0; g < groups.size(); g++) {
                encoders[g] = MultivalueDedupe.batchEncoder(page.getBlock(groups.get(g).channel()), batchSize);
            }
            bytes.grow(nullTrackingBytes);
            this.positionCount = page.getPositionCount();
        }

        /**
         * Encodes one permutation of the keys at time into {@link #bytes}. The encoding is
         * mostly provided by {@link BatchEncoder} with nulls living in a bit mask at the
         * front of the bytes.
         */
        void add() {
            for (position = 0; position < positionCount; position++) {
                // Make sure all encoders have encoded the current position and the offsets are queued to it's start
                boolean singleEntry = true;
                for (int g = 0; g < encoders.length; g++) {
                    positionOffsets[g]++;
                    if (positionOffsets[g] >= encoders[g].positionCount()) {
                        encoders[g].encodeNextBatch();
                        positionOffsets[g] = 0;
                        valueOffsets[g] = 0;
                    }
                    valueCounts[g] = encoders[g].valueCount(positionOffsets[g]);
                    singleEntry &= (valueCounts[g] == 1);
                }
                Arrays.fill(bytes.bytes(), 0, nullTrackingBytes, (byte) 0);
                bytes.setLength(nullTrackingBytes);
                if (singleEntry) {
                    addSingleEntry();
                } else {
                    addMultipleEntries();
                }
            }
            emitOrds();
        }

        private void addSingleEntry() {
            for (int g = 0; g < encoders.length; g++) {
                BytesRef v = encoders[g].read(valueOffsets[g]++, scratch);
                if (v.length == 0) {
                    int nullByte = g / 8;
                    int nullShift = g % 8;
                    bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
                } else {
                    bytes.append(v);
                }
            }
            int group = Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get())));
            ords.appendInt(group);
            addedValue(position);
        }

        private void addMultipleEntries() {
            ords.beginPositionEntry();
            int g = 0;
            outer: for (;;) {
                for (; g < encoders.length; g++) {
                    bytesStarts[g] = bytes.length();
                    BytesRef v = encoders[g].read(valueOffsets[g] + loopedIndices[g], scratch);
                    ++loopedIndices[g];
                    if (v.length == 0) {
                        int nullByte = g / 8;
                        int nullShift = g % 8;
                        bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
                    } else {
                        bytes.append(v);
                    }
                }
                // emit ords
                int group = Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get())));
                ords.appendInt(group);
                addedValueInMultivaluePosition(position);

                // rewind
                bytes.setLength(bytesStarts[--g]);
                while (loopedIndices[g] == valueCounts[g]) {
                    loopedIndices[g] = 0;
                    if (g == 0) {
                        break outer;
                    } else {
                        bytes.setLength(bytesStarts[--g]);
                    }
                }
            }
            ords.endPositionEntry();
            for (g = 0; g < encoders.length; g++) {
                valueOffsets[g] += valueCounts[g];
            }
        }
    }

    @Override
    public Block[] getKeys() {
        int size = Math.toIntExact(bytesRefHash.size());
        BatchEncoder.Decoder[] decoders = new BatchEncoder.Decoder[groups.size()];
        Block.Builder[] builders = new Block.Builder[groups.size()];
        for (int g = 0; g < builders.length; g++) {
            ElementType elementType = groups.get(g).elementType();
            decoders[g] = BatchEncoder.decoder(elementType);
            builders[g] = elementType.newBlockBuilder(size);
        }

        BytesRef[] values = new BytesRef[(int) Math.min(100, bytesRefHash.size())];
        BytesRef[] nulls = new BytesRef[values.length];
        for (int offset = 0; offset < values.length; offset++) {
            values[offset] = new BytesRef();
            nulls[offset] = new BytesRef();
            nulls[offset].length = nullTrackingBytes;
        }
        int offset = 0;
        for (int i = 0; i < bytesRefHash.size(); i++) {
            values[offset] = bytesRefHash.get(i, values[offset]);

            // Reference the null bytes in the nulls array and values in the values
            nulls[offset].bytes = values[offset].bytes;
            nulls[offset].offset = values[offset].offset;
            values[offset].offset += nullTrackingBytes;
            values[offset].length -= nullTrackingBytes;

            offset++;
            if (offset == values.length) {
                readKeys(decoders, builders, nulls, values, offset);
                offset = 0;
            }
        }
        if (offset > 0) {
            readKeys(decoders, builders, nulls, values, offset);
        }

        Block[] keyBlocks = new Block[groups.size()];
        for (int g = 0; g < keyBlocks.length; g++) {
            keyBlocks[g] = builders[g].build();
        }
        return keyBlocks;
    }

    private void readKeys(BatchEncoder.Decoder[] decoders, Block.Builder[] builders, BytesRef[] nulls, BytesRef[] values, int count) {
        for (int g = 0; g < builders.length; g++) {
            int nullByte = g / 8;
            int nullShift = g % 8;
            byte nullTest = (byte) (1 << nullShift);
            BatchEncoder.IsNull isNull = offset -> {
                BytesRef n = nulls[offset];
                return (n.bytes[n.offset + nullByte] & nullTest) != 0;
            };
            decoders[g].decode(builders[g], isNull, values, count);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()));
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(bytesRefHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        bytesRefHash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("PackedValuesBlockHash{groups=[");
        boolean first = true;
        for (HashAggregationOperator.GroupSpec spec : groups) {
            if (first) {
                first = false;
            } else {
                b.append(", ");
            }
            b.append(spec.channel()).append(':').append(spec.elementType());
        }
        b.append("], entries=").append(bytesRefHash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed()));
        return b.append("}").toString();
    }
}
