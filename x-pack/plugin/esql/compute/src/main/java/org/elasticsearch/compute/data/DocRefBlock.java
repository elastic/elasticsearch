/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around {@link DocRefVector} to make a valid {@link Block}. Unlike a {@link DocBlock} it can be written to the
 * wire.
 * <p>
 * Wire format, after the element type:
 * <ol>
 *     <li>{@code byte} flags, bit 0 is {@link DocRefVector#mayContainDuplicates()}</li>
 *     <li>{@code vint} position count</li>
 *     <li>{@code vint} origin count, {@code 0} only for an empty block</li>
 *     <li>the origins the rows reference, in the order rows first reference them</li>
 *     <li>with more than one origin, a {@code vint} origin ordinal per row</li>
 *     <li>the segments and the docs, as {@link IntVector}s</li>
 * </ol>
 * Origins no row references are not written.
 */
public final class DocRefBlock extends AbstractVectorBlock implements Block {
    /**
     * The first version that can read this block. The planner never sends it to an older node, so the check in
     * {@link Block#writeTypedBlock} only catches planner bugs.
     */
    public static final TransportVersion ESQL_DOC_REF = TransportVersion.fromName("esql_fetch_phase_plan");

    private static final byte FLAG_MAY_CONTAIN_DUPLICATES = 1;

    private final DocRefVector vector;

    DocRefBlock(DocRefVector vector) {
        this.vector = vector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        int positions = getPositionCount();
        DocRefOrigins origins = vector.origins();
        IntVector ordinals = vector.originOrdinals();

        // the ordinal each referenced origin gets on the wire, plus one so that zero marks an unreferenced origin
        int[] wireOrdinals = new int[origins.size()];
        List<DocRefOrigin> referenced = new ArrayList<>();
        int scan = ordinals.isConstant() ? Math.min(1, positions) : positions;
        for (int p = 0; p < scan; p++) {
            int ordinal = ordinals.getInt(p);
            if (wireOrdinals[ordinal] == 0) {
                referenced.add(origins.get(ordinal));
                wireOrdinals[ordinal] = referenced.size();
            }
        }

        out.writeByte(vector.mayContainDuplicates() ? FLAG_MAY_CONTAIN_DUPLICATES : 0);
        out.writeVInt(positions);
        out.writeVInt(referenced.size());
        for (DocRefOrigin origin : referenced) {
            origin.writeTo(out);
        }
        if (referenced.size() > 1) {
            for (int p = 0; p < positions; p++) {
                out.writeVInt(wireOrdinals[ordinals.getInt(p)] - 1);
            }
        }
        vector.segments().writeTo(out);
        vector.docs().writeTo(out);
    }

    /**
     * Reads a block written by {@link #writeTo}. The bytes come from another node, so every count, flag and ordinal is
     * checked. A corrupt block fails with an {@link IllegalStateException} and leaves nothing reserved on the breaker.
     */
    public static DocRefBlock readFrom(BlockStreamInput in) throws IOException {
        BlockFactory blockFactory = in.blockFactory();
        byte flags = in.readByte();
        if ((flags & ~FLAG_MAY_CONTAIN_DUPLICATES) != 0) {
            throw new IllegalStateException("unknown doc ref block flags [" + flags + "]");
        }
        int positions = in.readVInt();
        int originCount = in.readVInt();
        if (positions < 0 || (positions == 0 ? originCount != 0 : originCount < 1 || originCount > positions)) {
            throw new IllegalStateException("invalid doc ref block with [" + positions + "] positions and [" + originCount + "] origins");
        }
        // origins are reserved as they are read, so at most one is ever unaccounted
        long reserved = 0;
        IntVector ordinals = null;
        IntVector segments = null;
        IntVector docs = null;
        DocRefBlock result = null;
        try {
            // the counts are not trusted with allocations, the breaker accounts for the origins as they arrive
            List<DocRefOrigin> origins = new ArrayList<>();
            Set<DocRefOrigin> seen = new HashSet<>();
            for (int i = 0; i < originCount; i++) {
                DocRefOrigin origin = DocRefOrigin.readFrom(in);
                blockFactory.adjustBreaker(origin.ramBytesUsed());
                reserved += origin.ramBytesUsed();
                if (seen.add(origin) == false) {
                    throw new IllegalStateException("duplicate origin " + origin + " in doc ref block");
                }
                origins.add(origin);
            }
            ordinals = readOrdinals(blockFactory, in, positions, originCount);
            segments = IntVector.readFrom(blockFactory, in);
            docs = IntVector.readFrom(blockFactory, in);
            checkNonNegative("segments", segments, positions);
            checkNonNegative("docs", docs, positions);
            // the vector reserves the dictionary itself
            blockFactory.adjustBreaker(-reserved);
            reserved = 0;
            boolean mayContainDuplicates = (flags & FLAG_MAY_CONTAIN_DUPLICATES) != 0;
            result = new DocRefVector(DocRefOrigins.of(origins), ordinals, segments, docs, mayContainDuplicates).asBlock();
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, segments, docs);
            }
            if (reserved != 0) {
                blockFactory.adjustBreaker(-reserved);
            }
        }
    }

    private static IntVector readOrdinals(BlockFactory blockFactory, BlockStreamInput in, int positions, int originCount)
        throws IOException {
        if (originCount <= 1) {
            return blockFactory.newConstantIntVector(0, positions);
        }
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                int ordinal = in.readVInt();
                if (ordinal < 0 || ordinal >= originCount) {
                    throw new IllegalStateException("origin ordinal [" + ordinal + "] out of [0, " + originCount + ")");
                }
                builder.appendInt(ordinal);
            }
            return builder.build();
        }
    }

    private static void checkNonNegative(String name, IntVector vector, int positions) {
        if (vector.getPositionCount() != positions) {
            throw new IllegalStateException("expected [" + positions + "] " + name + " but got [" + vector.getPositionCount() + "]");
        }
        int scan = vector.isConstant() ? Math.min(1, positions) : positions;
        for (int p = 0; p < scan; p++) {
            if (vector.getInt(p) < 0) {
                throw new IllegalStateException("negative value [" + vector.getInt(p) + "] in " + name);
            }
        }
    }

    @Override
    public DocRefVector asVector() {
        return vector;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC_REF;
    }

    @Override
    public int valueMaxByteSize() {
        return vector.valueMaxByteSize();
    }

    @Override
    public DocRefBlock slice(int beginInclusive, int endExclusive) {
        return vector.slice(beginInclusive, endExclusive).asBlock();
    }

    @Override
    public DocRefBlock filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        return vector.filter(mayContainDuplicates, positions, offset, length).asBlock();
    }

    @Override
    public DocRefBlock filter(boolean mayContainDuplicates, int... positions) {
        return filter(mayContainDuplicates, positions, 0, positions.length);
    }

    @Override
    public DocRefBlock deepCopy(BlockFactory blockFactory) {
        return vector.deepCopy(blockFactory).asBlock();
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocRefBlock");
    }

    @Override
    public DocRefBlock expand() {
        incRef();
        return this;
    }

    @Override
    public Block insertNulls(IntVector before) {
        throw new UnsupportedOperationException("doc ref blocks can't contain null");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocRefBlock == false) {
            return false;
        }
        return this == obj || vector.equals(((DocRefBlock) obj).vector);
    }

    @Override
    public int hashCode() {
        return vector.hashCode();
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public void closeInternal() {
        assert vector.isReleased() == false : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        makeRefCountsThreadSafe();
        vector.allowPassingToDifferentDriver();
    }

    @Override
    public int getPositionCount() {
        return vector.getPositionCount();
    }

    @Override
    public BlockFactory blockFactory() {
        return vector.blockFactory();
    }

    @Override
    public String toString() {
        return "DocRefBlock[vector=" + vector + ']';
    }

    public static Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new Builder(blockFactory, estimatedSize);
    }

    /**
     * Builds a {@link DocRefBlock} from origins it interns and rows that point at them.
     */
    public static final class Builder implements Block.Builder {
        private final BlockFactory blockFactory;
        private final IntVector.Builder ordinals;
        private final IntVector.Builder segments;
        private final IntVector.Builder docs;
        private final Map<DocRefOrigin, Integer> ordinalsByOrigin = new HashMap<>();
        private final List<DocRefOrigin> origins = new ArrayList<>();
        private boolean mayContainDuplicates = true;

        // copyFrom usually reads many rows from one source block, so the source to builder ordinal map is kept
        private DocRefOrigins copySource;
        private int[] copySourceOrdinals;

        private Builder(BlockFactory blockFactory, int estimatedSize) {
            IntVector.Builder ordinals = null;
            IntVector.Builder segments = null;
            IntVector.Builder docs = null;
            try {
                ordinals = blockFactory.newIntVectorBuilder(estimatedSize);
                segments = blockFactory.newIntVectorBuilder(estimatedSize);
                docs = blockFactory.newIntVectorBuilder(estimatedSize);
            } finally {
                if (docs == null) {
                    Releasables.closeExpectNoException(ordinals, segments);
                }
            }
            this.blockFactory = blockFactory;
            this.ordinals = ordinals;
            this.segments = segments;
            this.docs = docs;
        }

        /**
         * Adds an origin to the dictionary if it isn't there yet.
         *
         * @return the ordinal rows use to point at the origin
         */
        public int addOrigin(DocRefOrigin origin) {
            Integer existing = ordinalsByOrigin.get(origin);
            if (existing != null) {
                return existing;
            }
            int ordinal = origins.size();
            origins.add(origin);
            ordinalsByOrigin.put(origin, ordinal);
            return ordinal;
        }

        /**
         * Appends a row.
         *
         * @param originOrdinal an ordinal returned by {@link #addOrigin}
         */
        public Builder append(int originOrdinal, int segment, int doc) {
            if (originOrdinal < 0 || originOrdinal >= origins.size()) {
                throw new IllegalArgumentException("origin ordinal [" + originOrdinal + "] out of [0, " + origins.size() + ")");
            }
            ordinals.appendInt(originOrdinal);
            segments.appendInt(segment);
            docs.appendInt(doc);
            return this;
        }

        /**
         * Can the block reference the same document twice? Defaults to {@code true}.
         */
        public Builder mayContainDuplicates(boolean mayContainDuplicates) {
            this.mayContainDuplicates = mayContainDuplicates;
            return this;
        }

        @Override
        public Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            DocRefVector source = ((DocRefBlock) block).asVector();
            if (source.origins() != copySource) {
                copySource = source.origins();
                copySourceOrdinals = new int[copySource.size()];
                Arrays.fill(copySourceOrdinals, -1);
            }
            for (int p = beginInclusive; p < endExclusive; p++) {
                int sourceOrdinal = source.originOrdinals().getInt(p);
                int ordinal = copySourceOrdinals[sourceOrdinal];
                if (ordinal < 0) {
                    // only origins that rows reference end up in the dictionary
                    ordinal = addOrigin(copySource.get(sourceOrdinal));
                    copySourceOrdinals[sourceOrdinal] = ordinal;
                }
                append(ordinal, source.segments().getInt(p), source.docs().getInt(p));
            }
            return this;
        }

        @Override
        public Builder appendNull() {
            throw new UnsupportedOperationException("doc ref blocks can't contain null");
        }

        @Override
        public Builder beginPositionEntry() {
            throw new UnsupportedOperationException("doc ref blocks only contain one value per position");
        }

        @Override
        public Builder endPositionEntry() {
            throw new UnsupportedOperationException("doc ref blocks only contain one value per position");
        }

        @Override
        public Builder mvOrdering(MvOrdering mvOrdering) {
            // copying calls this, but every position references exactly one document
            return this;
        }

        @Override
        public long estimatedBytes() {
            long bytes = DocRefVector.BASE_RAM_BYTES_USED + ordinals.estimatedBytes() + segments.estimatedBytes() + docs.estimatedBytes();
            for (DocRefOrigin origin : origins) {
                bytes += origin.ramBytesUsed();
            }
            return bytes;
        }

        @Override
        public DocRefBlock build() {
            IntVector builtOrdinals = null;
            IntVector builtSegments = null;
            IntVector builtDocs = null;
            DocRefBlock result = null;
            try {
                builtSegments = segments.build();
                builtDocs = docs.build();
                if (origins.size() == 1) {
                    // a constant ordinal vector also writes a single ordinal to the wire
                    ordinals.close();
                    builtOrdinals = blockFactory.newConstantIntVector(0, builtSegments.getPositionCount());
                } else {
                    builtOrdinals = ordinals.build();
                }
                result = new DocRefVector(DocRefOrigins.of(origins), builtOrdinals, builtSegments, builtDocs, mayContainDuplicates)
                    .asBlock();
                return result;
            } finally {
                if (result == null) {
                    Releasables.closeExpectNoException(builtOrdinals, builtSegments, builtDocs);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(ordinals, segments, docs);
        }
    }
}
