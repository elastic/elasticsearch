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
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around {@link DocVector} to make a valid {@link Block}.
 * <p>
 * Only a vector whose rows left the node that read them, one with {@link DocVector#origins()}, can be written. Wire
 * format, after the element type:
 * <ol>
 *     <li>{@code byte} flags, bit 0 is {@link DocVector#mayContainDuplicates()}</li>
 *     <li>{@code vint} position count</li>
 *     <li>{@code vint} origin count, {@code 0} only for an empty block</li>
 *     <li>the origins the rows reference, in the order rows first reference them</li>
 *     <li>with more than one origin, a {@code vint} origin ordinal per row</li>
 *     <li>the segments and the docs, as {@link IntVector}s</li>
 * </ol>
 * Origins no row references are not written.
 */
public class DocBlock extends AbstractVectorBlock implements Block, RefCounted {
    /**
     * The first version that can read this block. Code that sends one checks the version of the receiving node first, so
     * the check in {@link Block#writeTypedBlock} only catches bugs in that code.
     */
    public static final TransportVersion ESQL_DOC_ORIGINS = TransportVersion.fromName("esql_doc_origins");

    private static final byte FLAG_MAY_CONTAIN_DUPLICATES = 1;

    private final DocVector vector;

    DocBlock(DocVector vector) {
        this.vector = vector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        int positions = getPositionCount();
        DocOrigins origins = vector.origins();
        if (origins == null && positions > 0) {
            // the shard slots of a node mean nothing to another node
            throw new IllegalStateException("can't write documents that are still on the node that read them, detach them first");
        }
        IntVector ordinals = vector.shards();

        // the ordinal each referenced origin gets on the wire, plus one so that zero marks an unreferenced origin
        int[] wireOrdinals = new int[origins == null ? 0 : origins.size()];
        List<DocOrigin> referenced = new ArrayList<>();
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
        for (DocOrigin origin : referenced) {
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
    public static DocBlock readFrom(BlockStreamInput in) throws IOException {
        BlockFactory blockFactory = in.blockFactory();
        byte flags = in.readByte();
        if ((flags & ~FLAG_MAY_CONTAIN_DUPLICATES) != 0) {
            throw new IllegalStateException("unknown doc block flags [" + flags + "]");
        }
        int positions = in.readVInt();
        int originCount = in.readVInt();
        if (positions < 0 || (positions == 0 ? originCount != 0 : originCount < 1 || originCount > positions)) {
            throw new IllegalStateException("invalid doc block with [" + positions + "] positions and [" + originCount + "] origins");
        }
        // origins are reserved as they are read, so at most one is ever unaccounted
        long reserved = 0;
        IntVector ordinals = null;
        IntVector segments = null;
        IntVector docs = null;
        DocBlock result = null;
        try {
            // the counts are not trusted with allocations, the breaker accounts for the origins as they arrive
            List<DocOrigin> origins = new ArrayList<>();
            Set<DocOrigin> seen = new HashSet<>();
            for (int i = 0; i < originCount; i++) {
                DocOrigin origin = DocOrigin.readFrom(in);
                blockFactory.adjustBreaker(origin.ramBytesUsed());
                reserved += origin.ramBytesUsed();
                if (seen.add(origin) == false) {
                    throw new IllegalStateException("duplicate origin " + origin + " in doc block");
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
            DocVector.Config config = DocVector.config()
                .origins(DocOrigins.of(origins))
                .mayContainDuplicates((flags & FLAG_MAY_CONTAIN_DUPLICATES) != 0);
            result = new DocVector(AlwaysReferencedIndexedByShardId.INSTANCE, ordinals, segments, docs, config).asBlock();
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
    public DocVector asVector() {
        return vector;
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOC;
    }

    @Override
    public int valueMaxByteSize() {
        return 3 * Integer.BYTES;
    }

    @Override
    public Block slice(int beginInclusive, int endExclusive) {
        return vector.slice(beginInclusive, endExclusive).asBlock();
    }

    @Override
    public Block filter(boolean mayContainDuplicates, int[] positions, int offset, int length) {
        return new DocBlock(vector.filter(mayContainDuplicates, positions, offset, length));
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        return new DocBlock(vector.deepCopy(blockFactory));
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return vector.keepMask(mask);
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocBlock");
    }

    @Override
    public DocBlock expand() {
        incRef();
        return this;
    }

    @Override
    public int hashCode() {
        return vector.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DocBlock == false) {
            return false;
        }
        return this == obj || vector.equals(((DocBlock) obj).vector);
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public void closeInternal() {
        assert (vector.isReleased() == false) : "can't release block [" + this + "] containing already released vector";
        Releasables.closeExpectNoException(vector);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DocBlock[");
        sb.append("vector=").append(vector);
        sb.append(']');
        return sb.toString();
    }

    /**
     * A builder the for {@link DocBlock}.
     */
    public static Builder newBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new Builder(blockFactory, estimatedSize);
    }

    /**
     * Builds a {@link DocBlock}. Rows appended with {@link #appendShard} name shard slots of this node, unless the block
     * gets origins at {@link #build(DocVector.Config)}. {@link #copyFrom} keeps the kind of the rows it copies: slots stay
     * slots, and rows with origins get their origins interned in a dictionary of this builder. One block holds one kind.
     */
    public static class Builder implements Block.Builder {
        private final IntVector.Builder shards;
        private final IntVector.Builder segments;
        private final IntVector.Builder docs;
        private IndexedByShardId<? extends RefCounted> shardRefCounters = AlwaysReferencedIndexedByShardId.INSTANCE;

        /**
         * The dictionary of the rows {@link #copyFrom} copied from vectors with origins, {@code null} until it copies one.
         */
        @Nullable
        private Map<DocOrigin, Integer> copiedOrigins;
        private boolean copiedSlots;
        // copyFrom usually reads many rows from one source vector, so the source to builder ordinal map is kept
        private DocOrigins copySource;
        private int[] copySourceOrdinals;

        public Builder shardRefCounters(IndexedByShardId<? extends RefCounted> shardRefCounters) {
            this.shardRefCounters = shardRefCounters;
            return this;
        }

        private Builder(BlockFactory blockFactory, int estimatedSize) {
            IntVector.Builder shards = null;
            IntVector.Builder segments = null;
            IntVector.Builder docs = null;
            try {
                shards = blockFactory.newIntVectorBuilder(estimatedSize);
                segments = blockFactory.newIntVectorBuilder(estimatedSize);
                docs = blockFactory.newIntVectorBuilder(estimatedSize);
            } finally {
                if (docs == null) {
                    Releasables.closeExpectNoException(shards, segments, docs);
                }
            }
            this.shards = shards;
            this.segments = segments;
            this.docs = docs;
        }

        public Builder appendShard(int shard) {
            shards.appendInt(shard);
            return this;
        }

        public Builder appendSegment(int segment) {
            segments.appendInt(segment);
            return this;
        }

        public Builder appendDoc(int doc) {
            docs.appendInt(doc);
            return this;
        }

        @Override
        public Builder appendNull() {
            throw new UnsupportedOperationException("doc blocks can't contain null");
        }

        @Override
        public Builder beginPositionEntry() {
            throw new UnsupportedOperationException("doc blocks only contain one value per position");
        }

        @Override
        public Builder endPositionEntry() {
            throw new UnsupportedOperationException("doc blocks only contain one value per position");
        }

        @Override
        public Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            DocVector docVector = ((DocBlock) block).asVector();
            DocOrigins origins = docVector.origins();
            if (origins == null) {
                if (copiedOrigins != null) {
                    throw new IllegalStateException("can't mix documents of this node with documents that left their node");
                }
                copiedSlots |= endExclusive > beginInclusive;
                for (int i = beginInclusive; i < endExclusive; i++) {
                    shards.appendInt(docVector.shards().getInt(i));
                    segments.appendInt(docVector.segments().getInt(i));
                    docs.appendInt(docVector.docs().getInt(i));
                }
                return this;
            }
            if (copiedSlots) {
                throw new IllegalStateException("can't mix documents of this node with documents that left their node");
            }
            if (copiedOrigins == null) {
                copiedOrigins = new LinkedHashMap<>();
            }
            if (origins != copySource) {
                copySource = origins;
                copySourceOrdinals = new int[origins.size()];
                Arrays.fill(copySourceOrdinals, -1);
            }
            for (int i = beginInclusive; i < endExclusive; i++) {
                int sourceOrdinal = docVector.shards().getInt(i);
                int ordinal = copySourceOrdinals[sourceOrdinal];
                if (ordinal < 0) {
                    // only origins that rows reference end up in the dictionary
                    ordinal = copiedOrigins.computeIfAbsent(origins.get(sourceOrdinal), o -> copiedOrigins.size());
                    copySourceOrdinals[sourceOrdinal] = ordinal;
                }
                shards.appendInt(ordinal);
                segments.appendInt(docVector.segments().getInt(i));
                docs.appendInt(docVector.docs().getInt(i));
            }
            return this;
        }

        @Override
        public Block.Builder mvOrdering(MvOrdering mvOrdering) {
            /*
             * This is called when copying but otherwise doesn't do
             * anything because there aren't multivalue fields in a
             * block containing doc references. Every position can
             * only reference one doc.
             */
            return this;
        }

        @Override
        public long estimatedBytes() {
            return DocVector.BASE_RAM_BYTES_USED + shards.estimatedBytes() + segments.estimatedBytes() + docs.estimatedBytes();
        }

        @Override
        public DocBlock build() {
            return build(DocVector.config());
        }

        public DocBlock build(DocVector.Config config) {
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            DocVector result = null;
            try {
                shards = this.shards.build();
                segments = this.segments.build();
                docs = this.docs.build();
                if (copiedOrigins != null) {
                    // copied rows keep their origins, and a block with origins references no shard
                    config.origins(DocOrigins.of(List.copyOf(copiedOrigins.keySet())));
                    result = new DocVector(AlwaysReferencedIndexedByShardId.INSTANCE, shards, segments, docs, config);
                } else {
                    result = new DocVector(shardRefCounters, shards, segments, docs, config);
                }
                return result.asBlock();
            } finally {
                if (result == null) {
                    Releasables.closeExpectNoException(shards, segments, docs);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(shards, segments, docs);
        }
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
}
