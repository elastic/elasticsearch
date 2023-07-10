/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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
    private static final Logger logger = LogManager.getLogger(PackedValuesBlockHash.class);
    static final int DEFAULT_BATCH_SIZE = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());

    private final List<HashAggregationOperator.GroupSpec> groups;
    private final BytesRefHash bytesRefHash;
    private final int nullTrackingBytes;

    PackedValuesBlockHash(List<HashAggregationOperator.GroupSpec> groups, BigArrays bigArrays) {
        this.groups = groups;
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

    class AddWork {
        final BatchEncoder[] encoders = new BatchEncoder[groups.size()];
        final int[] positionOffsets = new int[groups.size()];
        final int[] valueOffsets = new int[groups.size()];
        final BytesRef[] scratches = new BytesRef[groups.size()];
        final BytesRefBuilder bytes = new BytesRefBuilder();
        final int positionCount;
        final GroupingAggregatorFunction.AddInput addInput;
        final LongBlock.Builder builder;

        int count;
        long bufferedGroup;

        AddWork(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            for (int g = 0; g < groups.size(); g++) {
                encoders[g] = MultivalueDedupe.batchEncoder(page.getBlock(groups.get(g).channel()), batchSize);
                scratches[g] = new BytesRef();
            }
            bytes.grow(nullTrackingBytes);
            this.positionCount = page.getPositionCount();
            this.addInput = addInput;
            builder = LongBlock.newBlockBuilder(positionCount);
        }

        /**
         * Encodes one permutation of the keys at time into {@link #bytes}. The encoding is
         * mostly provided by {@link BatchEncoder} with nulls living in a bit mask at the
         * front of the bytes.
         */
        void add() {
            for (int position = 0; position < positionCount; position++) {
                if (logger.isTraceEnabled()) {
                    logger.trace("position {}", position);
                }
                // Make sure all encoders have encoded the current position and the offsets are queued to it's start
                for (int g = 0; g < encoders.length; g++) {
                    positionOffsets[g]++;
                    while (positionOffsets[g] >= encoders[g].positionCount()) {
                        encoders[g].encodeNextBatch();
                        positionOffsets[g] = 0;
                        valueOffsets[g] = 0;
                    }
                }

                count = 0;
                Arrays.fill(bytes.bytes(), 0, nullTrackingBytes, (byte) 0);
                bytes.setLength(nullTrackingBytes);
                addPosition(0);
                switch (count) {
                    case 0 -> {
                        logger.trace("appending null");
                        builder.appendNull();  // TODO https://github.com/elastic/elasticsearch-internal/issues/1327
                    }
                    case 1 -> builder.appendLong(bufferedGroup);
                    default -> builder.endPositionEntry();
                }
                for (int g = 0; g < encoders.length; g++) {
                    valueOffsets[g] += encoders[g].valueCount(positionOffsets[g]);
                }
            }
            LongBlock groupIdsBlock = builder.build();  // TODO exploit for a crash and then call incrementally
            LongVector groupIdsVector = groupIdsBlock.asVector();
            if (groupIdsVector == null) {
                addInput.add(0, groupIdsBlock);
            } else {
                addInput.add(0, groupIdsVector);
            }
        }

        private void addPosition(int g) {
            if (g == groups.size()) {
                addBytes();
                return;
            }
            int start = bytes.length();
            int count = encoders[g].valueCount(positionOffsets[g]);
            assert count > 0;
            int valueOffset = valueOffsets[g];
            BytesRef v = encoders[g].read(valueOffset++, scratches[g]);
            if (logger.isTraceEnabled()) {
                logger.trace("\t".repeat(g + 1) + v);
            }
            if (v.length == 0) {
                assert count == 1 : "null value in non-singleton list";
                int nullByte = g / 8;
                int nullShift = g % 8;
                bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
            }
            bytes.setLength(start);
            bytes.append(v);
            addPosition(g + 1);  // TODO stack overflow protection
            for (int i = 1; i < count; i++) {
                v = encoders[g].read(valueOffset++, scratches[g]);
                if (logger.isTraceEnabled()) {
                    logger.trace("\t".repeat(g + 1) + v);
                }
                assert v.length > 0 : "null value after the first position";
                bytes.setLength(start);
                bytes.append(v);
                addPosition(g + 1);
            }
        }

        private void addBytes() {
            for (int i = 0; i < nullTrackingBytes; i++) {
                if (bytes.bytes()[i] != 0) {
                    // TODO https://github.com/elastic/elasticsearch-internal/issues/1327
                    return;
                }
            }
            long group = hashOrdToGroup(bytesRefHash.add(bytes.get()));
            switch (count) {
                case 0 -> bufferedGroup = group;
                case 1 -> {
                    builder.beginPositionEntry();
                    builder.appendLong(bufferedGroup);
                    builder.appendLong(group);
                }
                default -> builder.appendLong(group);
            }
            count++;
            if (logger.isTraceEnabled()) {
                logger.trace("{} = {}", bytes.get(), group);
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

        BytesRef values[] = new BytesRef[(int) Math.min(100, bytesRefHash.size())];
        for (int offset = 0; offset < values.length; offset++) {
            values[offset] = new BytesRef();
        }
        int offset = 0;
        for (int i = 0; i < bytesRefHash.size(); i++) {
            values[offset] = bytesRefHash.get(i, values[offset]);
            // TODO restore nulls. for now we're skipping them
            values[offset].offset += nullTrackingBytes;
            values[offset].length -= nullTrackingBytes;
            offset++;
            if (offset == values.length) {
                readKeys(decoders, builders, values, offset);
                offset = 0;
            }
        }
        if (offset > 0) {
            readKeys(decoders, builders, values, offset);
        }

        Block[] keyBlocks = new Block[groups.size()];
        for (int g = 0; g < keyBlocks.length; g++) {
            keyBlocks[g] = builders[g].build();
        }
        return keyBlocks;
    }

    private void readKeys(BatchEncoder.Decoder[] decoders, Block.Builder[] builders, BytesRef[] values, int count) {
        for (int g = 0; g < builders.length; g++) {
            decoders[g].decode(builders[g], values, count);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()));
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
