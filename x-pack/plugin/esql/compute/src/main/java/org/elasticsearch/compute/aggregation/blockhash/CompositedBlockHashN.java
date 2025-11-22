/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.IntNHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.List;

final class CompositedBlockHashN extends BlockHash {
    private final int emitBatchSize;
    private final IntNHash finalHash;
    private final BlockHash[] subHashes;
    private final int[] oneRow;
    private final List<GroupSpec> specs;

    CompositedBlockHashN(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize) {
        super(blockFactory);
        this.specs = specs;
        this.emitBatchSize = emitBatchSize;
        this.oneRow = new int[specs.size()];
        boolean success = false;
        this.subHashes = new BlockHash[specs.size()];
        try {
            this.finalHash = new IntNHash(1024, specs.size(), blockFactory.bigArrays());
            for (int i = 0; i < specs.size(); i++) {
                GroupSpec spec = specs.get(i);
                subHashes[i] = BlockHash.newForElementType(spec.channel(), spec.elementType(), blockFactory);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final IntBlock[] hashes = hashSubBlocks(page);
        try {
            if (allVectors(hashes)) {
                addSingleValue(page, addInput, hashes);
            } else {
                try (AddWork work = new AddWork(hashes, page, addInput)) {
                    work.add();
                }
            }
        } finally {
            Releasables.closeExpectNoException(hashes);
        }
    }

    private void addSingleValue(Page page, GroupingAggregatorFunction.AddInput addInput, IntBlock[] hashes) {
        int positionCount = page.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                for (int g = 0; g < hashes.length; g++) {
                    oneRow[g] = hashes[g].getInt(p);
                }
                long ord = hashOrdToGroup(finalHash.add(oneRow));
                builder.appendInt(Math.toIntExact(ord));
            }
            try (var ords = builder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    /**
     * The on-heap representation of a {@code for} loop for each group key.
     */
    private static class Group {
        final GroupSpec spec;
        int currentIndex;
        int firstIndex;
        int lastIndex;

        Group(GroupSpec spec) {
            this.spec = spec;
        }
    }

    class AddWork extends AddPage {
        final Group[] groups;
        final IntBlock[] hashes;
        final int positionCount;
        int position;

        AddWork(IntBlock[] hashes, Page page, GroupingAggregatorFunction.AddInput addInput) {
            super(blockFactory, emitBatchSize, addInput);
            this.positionCount = page.getPositionCount();
            this.groups = new Group[specs.size()];
            this.hashes = hashes;
            for (int g = 0; g < specs.size(); g++) {
                groups[g] = new Group(specs.get(g));
            }
        }

        void add() {
            for (position = 0; position < positionCount; position++) {
                boolean singleEntry = startNewPosition();
                if (singleEntry) {
                    long ord = hashOrdToGroup(finalHash.add(oneRow));
                    appendOrdSv(position, Math.toIntExact(ord));
                } else {
                    addMultipleEntries();
                }
            }
            flushRemaining();
        }

        private boolean startNewPosition() {
            boolean singleEntry = true;
            for (int g = 0; g < groups.length; g++) {
                Group group = groups[g];
                IntBlock hash = hashes[g];
                group.firstIndex = hash.getFirstValueIndex(position);
                int valueCount = hash.getValueCount(position);
                group.lastIndex = group.firstIndex + valueCount - 1;
                group.currentIndex = group.firstIndex;
                if (valueCount != 1) {
                    singleEntry = false;
                }
                oneRow[g] = hash.getInt(group.currentIndex++);
            }
            return singleEntry;
        }

        private void addMultipleEntries() {
            do {
                long ord = hashOrdToGroup(finalHash.add(oneRow));
                appendOrdInMv(position, Math.toIntExact(ord));
            } while (rewindKeys());
            finishMv();
        }

        private boolean rewindKeys() {
            for (int g = groups.length - 1; g >= 0; g--) {
                Group group = groups[g];
                if (group.currentIndex <= group.lastIndex) {
                    oneRow[g] = hashes[g].getInt(group.currentIndex++);
                    return true;
                } else {
                    oneRow[g] = hashes[g].getInt(group.firstIndex);
                    group.currentIndex = group.firstIndex + 1;
                }
            }
            return false;
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return new LookupWork(page, targetBlockSize.getBytes());
    }

    class LookupWork implements ReleasableIterator<IntBlock> {
        private final Group[] groups;
        private final IntBlock[] hashes;
        private final long targetByteSize;
        private final int positionCount;
        private int position;

        LookupWork(Page page, long targetByteSize) {
            this.groups = specs.stream().map(Group::new).toArray(Group[]::new);
            this.hashes = hashSubBlocks(page);
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
                if (allVectors(hashes)) {
                    lookupSingleValues(ords, size);
                } else {
                    while (position < positionCount && ords.estimatedBytes() < targetByteSize) {
                        boolean singleEntry = startNewPosition();
                        if (singleEntry) {
                            lookupSingleEntry(ords);
                        } else {
                            lookupMultipleEntries(ords);
                        }
                        position++;
                    }
                }
                return ords.build();
            }
        }

        void lookupSingleValues(IntBlock.Builder ords, int size) {
            for (int i = 0; i < size; i++) {
                for (int g = 0; g < hashes.length; g++) {
                    oneRow[g] = hashes[g].getInt(position);
                }
                long found = finalHash.find(oneRow);
                if (found < 0) {
                    ords.appendNull();
                } else {
                    ords.appendInt(Math.toIntExact(found));
                }
                position++;
            }
        }

        private boolean startNewPosition() {
            boolean singleEntry = true;
            for (int g = 0; g < groups.length; g++) {
                Group group = groups[g];
                IntBlock hash = hashes[g];
                group.firstIndex = hash.getFirstValueIndex(position);
                int valueCount = hash.getValueCount(position);
                group.lastIndex = group.firstIndex + valueCount - 1;
                group.currentIndex = group.firstIndex;
                if (valueCount != 1) {
                    singleEntry = false;
                }
                oneRow[g] = hash.getInt(group.currentIndex++);
            }
            return singleEntry;
        }

        private boolean rewindKeys() {
            for (int g = groups.length - 1; g >= 0; g--) {
                Group group = groups[g];
                if (group.currentIndex <= group.lastIndex) {
                    oneRow[g] = hashes[g].getInt(group.currentIndex++);
                    return true;
                } else {
                    oneRow[g] = hashes[g].getInt(group.firstIndex);
                    group.currentIndex = group.firstIndex + 1;
                }
            }
            return false;
        }

        private void lookupSingleEntry(IntBlock.Builder ords) {
            long found = finalHash.find(oneRow);
            if (found < 0) {
                ords.appendNull();
            } else {
                ords.appendInt(Math.toIntExact(found));
            }
        }

        private void lookupMultipleEntries(IntBlock.Builder ords) {
            long found = finalHash.find(oneRow);
            if (found < 0) {
                ords.appendNull();
            } else if (rewindKeys()) {
                ords.beginPositionEntry();
                ords.appendInt(Math.toIntExact(found));
                do {
                    long ord = finalHash.find(oneRow);
                    ords.appendInt(Math.toIntExact(ord));
                } while (rewindKeys());
                ords.endPositionEntry();
            } else {
                ords.appendInt(Math.toIntExact(found));
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(hashes);
        }
    }

    @Override
    public Block[] getKeys() {
        int size = Math.toIntExact(finalHash.size());
        KeyBuilder[] builders = new KeyBuilder[specs.size()];
        Block[] blocks = new Block[builders.length];
        boolean success = false;
        try {
            for (int g = 0; g < specs.size(); g++) {
                builders[g] = createKeyBuilders(specs.get(g), subHashes[g], size);
            }
            for (int p = 0; p < finalHash.size(); p++) {
                finalHash.getKeys(p, oneRow);
                for (int g = 0; g < builders.length; g++) {
                    int hash = oneRow[g];
                    if (hash == 0) {
                        builders[g].appendNull();
                    } else {
                        builders[g].addRow(hash - 1);
                    }
                }
            }
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build();
                assert blocks[i] != null : specs.get(i);
            }
            success = true;
            return blocks;
        } finally {
            Releasables.closeExpectNoException(builders);
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(finalHash.size()), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new Range(0, Math.toIntExact(finalHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        finalHash.close();
        Releasables.close(subHashes);
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
        b.append("], entries=").append(finalHash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(finalHash.size()));
        return b.append("}").toString();
    }

    private static boolean allVectors(IntBlock[] hashes) {
        for (IntBlock hash : hashes) {
            if (hash.asVector() == null) {
                return false;
            }
        }
        return true;
    }

    private IntBlock[] hashSubBlocks(Page page) {
        IntBlock[] hashes = new IntBlock[specs.size()];
        boolean success = false;
        try {
            for (int i = 0; i < specs.size(); i++) {
                GroupSpec spec = specs.get(i);
                hashes[i] = hashOneBlock(spec.elementType(), subHashes[i], page.getBlock(spec.channel()));
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(hashes);
            }
        }
        return hashes;
    }

    private IntBlock hashOneBlock(ElementType elementType, BlockHash blockHash, Block input) {
        if (input.areAllValuesNull()) {
            return blockFactory.newConstantIntVector(0, input.getPositionCount()).asBlock();
        }
        return switch (elementType) {
            case BOOLEAN -> ((BooleanBlockHash) blockHash).add((BooleanBlock) input);
            case INT -> ((IntBlockHash) blockHash).add((IntBlock) input);
            case LONG -> ((LongBlockHash) blockHash).add((LongBlock) input);
            case DOUBLE -> ((DoubleBlockHash) blockHash).add((DoubleBlock) input);
            case BYTES_REF -> ((BytesRefBlockHash) blockHash).add((BytesRefBlock) input);
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + elementType + "]");
        };
    }

    private KeyBuilder createKeyBuilders(GroupSpec group, BlockHash blockHash, int estimatedSize) {
        return switch (group.elementType()) {
            case BOOLEAN -> new BoolKeyBuilder(blockFactory, estimatedSize);
            case INT -> new IntKeyBuilder(blockFactory, blockHash, estimatedSize);
            case LONG -> new LongKeyBuilder(blockFactory, blockHash, estimatedSize);
            case DOUBLE -> new DoubleKeyBuilder(blockFactory, blockHash, estimatedSize);
            case BYTES_REF -> new BytesRefKeyBuilder(blockFactory, blockHash, estimatedSize);
            case NULL -> new NullKeyBuilder(blockFactory);
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + group.elementType() + "]");
        };
    }

    abstract static class KeyBuilder implements Releasable {
        abstract void addRow(int hash);

        abstract void appendNull();

        abstract Block build();
    }

    static final class BoolKeyBuilder extends KeyBuilder {
        private final BooleanBlock.Builder builder;

        BoolKeyBuilder(BlockFactory blockFactory, int estimatedPosition) {
            this.builder = blockFactory.newBooleanBlockBuilder(estimatedPosition);
        }

        @Override
        void addRow(int hash) {
            builder.appendBoolean(MultivalueDedupeBoolean.TRUE_ORD == (hash + 1));
        }

        @Override
        void appendNull() {
            builder.appendNull();
        }

        @Override
        Block build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static final class NullKeyBuilder extends KeyBuilder {
        private final BlockFactory blockFactory;
        private int positions = 0;

        NullKeyBuilder(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        void addRow(int hash) {
            assert false;
            throw new IllegalStateException();
        }

        @Override
        void appendNull() {
            positions++;
        }

        @Override
        Block build() {
            return blockFactory.newConstantNullBlock(positions);
        }

        @Override
        public void close() {

        }
    }

    static final class IntKeyBuilder extends KeyBuilder {
        private final IntBlockHash blockHash;
        private final IntBlock.Builder builder;

        IntKeyBuilder(BlockFactory blockFactory, BlockHash blockHash, int estimatedPosition) {
            this.blockHash = (IntBlockHash) blockHash;
            this.builder = blockFactory.newIntBlockBuilder(estimatedPosition);
        }

        @Override
        void addRow(int hash) {
            builder.appendInt(Math.toIntExact(blockHash.hash.get(hash)));
        }

        @Override
        void appendNull() {
            builder.appendNull();
        }

        @Override
        Block build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static final class LongKeyBuilder extends KeyBuilder {
        private final LongBlockHash blockHash;
        private final LongBlock.Builder builder;

        LongKeyBuilder(BlockFactory blockFactory, BlockHash blockHash, int estimatedPosition) {
            this.blockHash = (LongBlockHash) blockHash;
            this.builder = blockFactory.newLongBlockBuilder(estimatedPosition);
        }

        @Override
        void addRow(int hash) {
            builder.appendLong(blockHash.hash.get(hash));
        }

        @Override
        void appendNull() {
            builder.appendNull();
        }

        @Override
        Block build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static final class DoubleKeyBuilder extends KeyBuilder {
        private final DoubleBlockHash blockHash;
        private final DoubleBlock.Builder builder;

        DoubleKeyBuilder(BlockFactory blockFactory, BlockHash blockHash, int estimatedPosition) {
            this.blockHash = (DoubleBlockHash) blockHash;
            this.builder = blockFactory.newDoubleBlockBuilder(estimatedPosition);
        }

        @Override
        void addRow(int hash) {
            builder.appendDouble(Double.longBitsToDouble(blockHash.hash.get(hash)));
        }

        @Override
        void appendNull() {
            builder.appendNull();
        }

        @Override
        Block build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static final class BytesRefKeyBuilder extends KeyBuilder {
        private final BytesRefBlockHash blockHash;
        private final BytesRefBlock.Builder builder;
        private final BytesRef scratch = new BytesRef();

        BytesRefKeyBuilder(BlockFactory blockFactory, BlockHash blockHash, int estimatedPosition) {
            this.blockHash = (BytesRefBlockHash) blockHash;
            this.builder = blockFactory.newBytesRefBlockBuilder(estimatedPosition);
        }

        @Override
        void addRow(int hash) {
            var val = blockHash.hash.get(hash, scratch);
            builder.appendBytesRef(val);
        }

        @Override
        void appendNull() {
            builder.appendNull();
        }

        @Override
        Block build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }
}
