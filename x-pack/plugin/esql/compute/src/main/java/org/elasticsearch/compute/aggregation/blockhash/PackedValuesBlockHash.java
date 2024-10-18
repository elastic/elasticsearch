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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.BatchEncoder;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

/**
 * Maps any number of columns to a group ids with every unique combination resulting
 * in a unique group id. Works by unique-ing the values of each column and concatenating
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
 * <p>
 *     The iteration order in the above is how we do it - it's as though it's
 *     nested {@code for} loops with the first column being the outer-most loop
 *     and the last column being the inner-most loop. See {@link Group} for more.
 * </p>
 */
final class PackedValuesBlockHash extends BlockHash {
    static final int DEFAULT_BATCH_SIZE = Math.toIntExact(ByteSizeValue.ofKb(10).getBytes());

    private final int emitBatchSize;
    private final BytesRefHash bytesRefHash;
    private final int nullTrackingBytes;
    private final BytesRefBuilder bytes = new BytesRefBuilder();
    private final List<GroupSpec> specs;

    PackedValuesBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize) {
        super(blockFactory);
        this.specs = specs;
        this.emitBatchSize = emitBatchSize;
        this.bytesRefHash = new BytesRefHash(1, blockFactory.bigArrays());
        this.nullTrackingBytes = (specs.size() + 7) / 8;
        bytes.grow(nullTrackingBytes);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        add(page, addInput, DEFAULT_BATCH_SIZE);
    }

    void add(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
        try (AddWork work = new AddWork(page, addInput, batchSize)) {
            work.add();
        }
    }

    /**
     * The on-heap representation of a {@code for} loop for each group key.
     */
    private static class Group implements Releasable {
        final GroupSpec spec;
        final BatchEncoder encoder;
        int positionOffset;
        int valueOffset;
        /**
         * The number of values we've written for this group. Think of it as
         * the loop variable in a {@code for} loop.
         */
        int writtenValues;
        /**
         * The number of values of this group at this position. Think of it as
         * the maximum value in a {@code for} loop.
         */
        int valueCount;
        int bytesStart;

        Group(GroupSpec spec, Page page, int batchSize) {
            this.spec = spec;
            this.encoder = MultivalueDedupe.batchEncoder(page.getBlock(spec.channel()), batchSize, true);
        }

        @Override
        public void close() {
            encoder.close();
        }
    }

    class AddWork extends AddPage {
        final Group[] groups;
        final int positionCount;
        int position;

        AddWork(Page page, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            super(blockFactory, emitBatchSize, addInput);
            this.groups = specs.stream().map(s -> new Group(s, page, batchSize)).toArray(Group[]::new);
            this.positionCount = page.getPositionCount();
        }

        /**
         * Encodes one permutation of the keys at time into {@link #bytes} and adds it
         * to the {@link #bytesRefHash}. The encoding is mostly provided by
         * {@link BatchEncoder} with nulls living in a bit mask at the front of the bytes.
         */
        void add() {
            for (position = 0; position < positionCount; position++) {
                boolean singleEntry = startPosition(groups);
                if (singleEntry) {
                    addSingleEntry();
                } else {
                    addMultipleEntries();
                }
            }
            flushRemaining();
        }

        private void addSingleEntry() {
            fillBytesSv(groups);
            appendOrdSv(position, Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get()))));
        }

        private void addMultipleEntries() {
            int g = 0;
            do {
                fillBytesMv(groups, g);
                appendOrdInMv(position, Math.toIntExact(hashOrdToGroup(bytesRefHash.add(bytes.get()))));
                g = rewindKeys(groups);
            } while (g >= 0);
            finishMv();
            for (Group group : groups) {
                group.valueOffset += group.valueCount;
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(super::close, Releasables.wrap(groups));
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return new LookupWork(page, targetBlockSize.getBytes(), DEFAULT_BATCH_SIZE);
    }

    class LookupWork implements ReleasableIterator<IntBlock> {
        private final Group[] groups;
        private final long targetByteSize;
        private final int positionCount;
        private int position;

        LookupWork(Page page, long targetByteSize, int batchSize) {
            this.groups = specs.stream().map(s -> new Group(s, page, batchSize)).toArray(Group[]::new);
            this.positionCount = page.getPositionCount();
            this.targetByteSize = targetByteSize;
        }

        @Override
        public boolean hasNext() {
            return position < positionCount;
        }

        @Override
        public IntBlock next() {
            int size = Math.toIntExact(Math.min(positionCount - position, targetByteSize / Integer.BYTES / 2));
            try (IntBlock.Builder ords = blockFactory.newIntBlockBuilder(size)) {
                if (ords.estimatedBytes() > targetByteSize) {
                    throw new IllegalStateException(
                        "initial builder overshot target [" + ords.estimatedBytes() + "] vs [" + targetByteSize + "]"
                    );
                }
                while (position < positionCount && ords.estimatedBytes() < targetByteSize) {
                    // TODO a test where targetByteSize is very small should still make a few rows.
                    boolean singleEntry = startPosition(groups);
                    if (singleEntry) {
                        lookupSingleEntry(ords);
                    } else {
                        lookupMultipleEntries(ords);
                    }
                    position++;
                }
                return ords.build();
            }
        }

        private void lookupSingleEntry(IntBlock.Builder ords) {
            fillBytesSv(groups);
            long found = bytesRefHash.find(bytes.get());
            if (found < 0) {
                ords.appendNull();
            } else {
                ords.appendInt(Math.toIntExact(found));
            }
        }

        private void lookupMultipleEntries(IntBlock.Builder ords) {
            long firstFound = -1;
            boolean began = false;
            int g = 0;
            int count = 0;
            do {
                fillBytesMv(groups, g);

                // emit ords
                long found = bytesRefHash.find(bytes.get());
                if (found >= 0) {
                    if (firstFound < 0) {
                        firstFound = found;
                    } else {
                        if (began == false) {
                            began = true;
                            ords.beginPositionEntry();
                            ords.appendInt(Math.toIntExact(firstFound));
                            count++;
                        }
                        ords.appendInt(Math.toIntExact(found));
                        count++;
                        if (count > Block.MAX_LOOKUP) {
                            // TODO replace this with a warning and break
                            throw new IllegalArgumentException("Found a single entry with " + count + " entries");
                        }
                    }
                }
                g = rewindKeys(groups);
            } while (g >= 0);
            if (firstFound < 0) {
                ords.appendNull();
            } else if (began) {
                ords.endPositionEntry();
            } else {
                // Only found one value
                ords.appendInt(Math.toIntExact(hashOrdToGroup(firstFound)));
            }
            for (Group group : groups) {
                group.valueOffset += group.valueCount;
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(groups);
        }
    }

    /**
     * Correctly position all {@code groups}, clear the {@link #bytes},
     * and position it past the null tracking bytes. Call this before
     * encoding a new position.
     * @return true if this position has only a single ordinal
     */
    private boolean startPosition(Group[] groups) {
        boolean singleEntry = true;
        for (Group g : groups) {
            /*
             * Make sure all encoders have encoded the current position and the
             * offsets are queued to its start.
             */
            var encoder = g.encoder;
            g.positionOffset++;
            while (g.positionOffset >= encoder.positionCount()) {
                encoder.encodeNextBatch();
                g.positionOffset = 0;
                g.valueOffset = 0;
            }
            g.valueCount = encoder.valueCount(g.positionOffset);
            singleEntry &= (g.valueCount == 1);
        }
        Arrays.fill(bytes.bytes(), 0, nullTrackingBytes, (byte) 0);
        bytes.setLength(nullTrackingBytes);
        return singleEntry;
    }

    private void fillBytesSv(Group[] groups) {
        for (int g = 0; g < groups.length; g++) {
            Group group = groups[g];
            assert group.writtenValues == 0;
            assert group.valueCount == 1;
            if (group.encoder.read(group.valueOffset++, bytes) == 0) {
                int nullByte = g / 8;
                int nullShift = g % 8;
                bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
            }
        }
    }

    private void fillBytesMv(Group[] groups, int startingGroup) {
        for (int g = startingGroup; g < groups.length; g++) {
            Group group = groups[g];
            group.bytesStart = bytes.length();
            if (group.encoder.read(group.valueOffset + group.writtenValues, bytes) == 0) {
                assert group.valueCount == 1 : "null value in non-singleton list";
                int nullByte = g / 8;
                int nullShift = g % 8;
                bytes.bytes()[nullByte] |= (byte) (1 << nullShift);
            }
            ++group.writtenValues;
        }
    }

    private int rewindKeys(Group[] groups) {
        int g = groups.length - 1;
        Group group = groups[g];
        bytes.setLength(group.bytesStart);
        while (group.writtenValues == group.valueCount) {
            group.writtenValues = 0;
            if (g == 0) {
                return -1;
            } else {
                group = groups[--g];
                bytes.setLength(group.bytesStart);
            }
        }
        return g;
    }

    @Override
    public Block[] getKeys() {
        int size = Math.toIntExact(bytesRefHash.size());
        BatchEncoder.Decoder[] decoders = new BatchEncoder.Decoder[specs.size()];
        Block.Builder[] builders = new Block.Builder[specs.size()];
        try {
            for (int g = 0; g < builders.length; g++) {
                ElementType elementType = specs.get(g).elementType();
                decoders[g] = BatchEncoder.decoder(elementType);
                builders[g] = elementType.newBlockBuilder(size, blockFactory);
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
            return Block.Builder.buildAll(builders);
        } finally {
            Releasables.closeExpectNoException(builders);
        }
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
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()), blockFactory);
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
        for (int i = 0; i < specs.size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            GroupSpec spec = specs.get(i);
            b.append(spec.channel()).append(':').append(spec.elementType());
        }
        b.append("], entries=").append(bytesRefHash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed()));
        return b.append("}").toString();
    }
}
