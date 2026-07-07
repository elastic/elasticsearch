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
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
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
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A {@link BlockHash} for groupings shaped like {@code (prefix, tail1, tail2, ..., tailN)} where
 * the {@code tail} columns, taken together as one tuple, recur unchanged across many different
 * {@code prefix} values - the classic shape of a time-bucketed aggregate
 * ({@code STATS ... BY bucket(...), dim1, dim2, ..., dimN}, the shape produced for the
 * coordinator-side aggregate by {@code TranslateTimeSeriesAggregate}), but not limited to it: any
 * grouping where one column varies often while the rest of the tuple repeats fits this shape.
 * <p>
 * The generic fallback for wide/mixed-type groupings, {@link PackedValuesBlockHash}, flattens
 * {@code (prefix, tail1, ..., tailN)} into one composite byte string per row and hashes that
 * directly. When the tail repeats, this is wasteful: the same tail bytes get physically re-copied
 * once per distinct prefix value the tail is paired with, instead of once per distinct tail.
 * <p>
 * This class avoids that by hashing the tail tuple through its own dictionary first, reducing it
 * to a small integer ordinal, and storing the raw tail bytes only once per distinct tail. The
 * outer hash that actually assigns group ids is then a cheap two-{@code long} hash keyed on
 * {@code (tailOrdinal, prefix)} - the same dictionary pattern {@link TimeSeriesBlockHash} already
 * uses for {@code (tsid, timestamp)}.
 * <p>
 * This is a proof-of-concept, not production ready. Known gaps, tracked as TODOs below and in
 * {@link BlockHash#build}:
 * <ul>
 *   <li>TODO: tail columns must currently be single-valued - a multi-valued tail column throws
 *       {@link UnsupportedOperationException} instead of doing {@link PackedValuesBlockHash}'s
 *       correct combinatorial expansion. Needed before this can replace the generic fallback for
 *       real, unscoped traffic.</li>
 *   <li>TODO: {@link #lookup} is unimplemented (mirrors the same gap in {@link TimeSeriesBlockHash}
 *       for the same reason - only {@code add} is exercised by the aggregate use case this targets).
 *       Needs a real implementation if this is ever used somewhere lookups matter (e.g. a join).</li>
 *   <li>TODO: no cardinality/size-based fallback - if the tail turns out to have little or no
 *       repetition in practice, this pays a small extra indirection (the dictionary lookup) for no
 *       benefit over {@link PackedValuesBlockHash}. Not currently a problem for the query this was
 *       built for, but worth measuring before using this shape more broadly.</li>
 *   <li>TODO: not wired into the planner. Currently only reachable via a temporary, unsafe dispatch
 *       hook in {@link BlockHash#build} - see the TODO there for what real wiring should look like.</li>
 * </ul>
 */
public final class PrefixBlockHash extends BlockHash {

    private static final TopNEncoder ENCODER = TopNEncoder.DEFAULT_UNSORTABLE;

    private final int prefixChannel;
    private final ElementType prefixType;
    private final List<GroupSpec> tailSpecs;
    private final int nullTrackingBytes;

    // Package-private (not private) so tests can inspect ramBytesUsed() directly, matching the same
    // precedent as PackedValuesBlockHash#bytesRefHash.
    final BytesRefHashTable tailDictionary;
    final LongLongHashTable outerHash;
    private final BreakingBytesRefBuilder packBuffer;
    private final BytesRef scratch = new BytesRef();

    public PrefixBlockHash(int prefixChannel, ElementType prefixType, List<GroupSpec> tailSpecs, BlockFactory blockFactory) {
        super(blockFactory);
        if (prefixType != ElementType.LONG && prefixType != ElementType.INT) {
            throw new IllegalArgumentException("prefix column must be LONG or INT, got [" + prefixType + "]");
        }
        this.prefixChannel = prefixChannel;
        this.prefixType = prefixType;
        this.tailSpecs = tailSpecs;
        this.nullTrackingBytes = (tailSpecs.size() + 7) / 8;
        boolean success = false;
        try {
            this.tailDictionary = HashImplFactory.newBytesRefHash(blockFactory);
            this.outerHash = HashImplFactory.newLongLongHash(blockFactory);
            this.packBuffer = new BreakingBytesRefBuilder(blockFactory.breaker(), "PrefixBlockHash", 128);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(tailDictionary, outerHash, packBuffer);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final int positionCount = page.getPositionCount();
        final Block prefixBlock = page.getBlock(prefixChannel);
        final Block[] tailBlocks = new Block[tailSpecs.size()];
        for (int g = 0; g < tailSpecs.size(); g++) {
            tailBlocks[g] = page.getBlock(tailSpecs.get(g).channel());
        }
        try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (prefixBlock.isNull(p)) {
                    throw new UnsupportedOperationException("null prefix values are not supported by PrefixBlockHash");
                }
                long prefixValue = readPrefixValue(prefixBlock, prefixBlock.getFirstValueIndex(p));
                long ordinal = hashOrdToGroup(tailDictionary.add(packTail(tailBlocks, p)));
                long groupId = hashOrdToGroup(outerHash.add(ordinal, prefixValue));
                groupIdsBuilder.appendInt(p, Math.toIntExact(groupId));
            }
            try (var groupIds = groupIdsBuilder.build()) {
                addInput.add(0, groupIds);
            }
        }
    }

    private long readPrefixValue(Block block, int valueIndex) {
        return switch (prefixType) {
            case LONG -> ((LongBlock) block).getLong(valueIndex);
            case INT -> ((IntBlock) block).getInt(valueIndex);
            default -> throw new IllegalStateException("unreachable: prefix type checked in constructor");
        };
    }

    /** Packs one row's tail values into {@link #packBuffer}, returning a view of it. */
    private BytesRef packTail(Block[] tailBlocks, int position) {
        packBuffer.clear();
        byte[] nullBitmap = new byte[nullTrackingBytes];
        for (int g = 0; g < tailBlocks.length; g++) {
            if (tailBlocks[g].isNull(position)) {
                nullBitmap[g >> 3] |= (byte) (1 << (g & 7));
            }
        }
        packBuffer.append(nullBitmap, 0, nullTrackingBytes);
        for (int g = 0; g < tailBlocks.length; g++) {
            Block block = tailBlocks[g];
            if (block.isNull(position)) {
                continue;
            }
            if (block.getValueCount(position) != 1) {
                throw new UnsupportedOperationException("PrefixBlockHash does not support multi-valued tail columns yet");
            }
            int valueIndex = block.getFirstValueIndex(position);
            switch (block.elementType()) {
                case BYTES_REF -> ENCODER.encodeBytesRef(((BytesRefBlock) block).getBytesRef(valueIndex, scratch), packBuffer);
                case LONG -> ENCODER.encodeLong(((LongBlock) block).getLong(valueIndex), packBuffer);
                case INT -> ENCODER.encodeInt(((IntBlock) block).getInt(valueIndex), packBuffer);
                case DOUBLE -> ENCODER.encodeDouble(((DoubleBlock) block).getDouble(valueIndex), packBuffer);
                case BOOLEAN -> ENCODER.encodeBoolean(((BooleanBlock) block).getBoolean(valueIndex), packBuffer);
                default -> throw new IllegalStateException("unsupported tail element type [" + block.elementType() + "]");
            }
        }
        return packBuffer.bytesRefView();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        // Not needed for the STATS/aggregate use case this class targets (mirrors TimeSeriesBlockHash,
        // which has the same gap for the same reason: only GroupingAggregatorFunction#add is used here).
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        final int positionCount = selected.getPositionCount();
        final Block[] result = new Block[1 + tailSpecs.size()];

        if (prefixType == ElementType.LONG) {
            try (var b = blockFactory.newLongVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    b.appendLong(p, outerHash.getKey2(selected.getInt(p)));
                }
                result[0] = b.build().asBlock();
            }
        } else {
            try (var b = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    b.appendInt(p, Math.toIntExact(outerHash.getKey2(selected.getInt(p))));
                }
                result[0] = b.build().asBlock();
            }
        }

        final Block.Builder[] builders = new Block.Builder[tailSpecs.size()];
        for (int g = 0; g < tailSpecs.size(); g++) {
            builders[g] = newBuilderFor(tailSpecs.get(g).elementType(), positionCount);
        }
        boolean success = false;
        try {
            BytesRef dictScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                long ordinal = outerHash.getKey1(selected.getInt(p));
                BytesRef packed = tailDictionary.get(ordinal, dictScratch);
                unpackTail(packed, builders);
            }
            for (int g = 0; g < tailSpecs.size(); g++) {
                result[1 + g] = builders[g].build();
            }
            success = true;
        } finally {
            // Builders must be closed whether or not build() was called on them - ownership of their
            // backing arrays transfers to the built Block on success, this is just releasing the
            // (now-empty) builder itself, matching the try-with-resources pattern used everywhere else
            // in this codebase (e.g. InternalPacks/PackedValuesBlockHash).
            Releasables.close(builders);
            if (success == false) {
                Releasables.close(result[0]);
            }
        }
        return result;
    }

    private Block.Builder newBuilderFor(ElementType type, int positionCount) {
        return switch (type) {
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(positionCount);
            case LONG -> blockFactory.newLongBlockBuilder(positionCount);
            case INT -> blockFactory.newIntBlockBuilder(positionCount);
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(positionCount);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(positionCount);
            default -> throw new IllegalStateException("unsupported tail element type [" + type + "]");
        };
    }

    /** Reverses {@link #packTail}: decodes one tail tuple's packed bytes back into typed columns. */
    private void unpackTail(BytesRef packed, Block.Builder[] builders) {
        BytesRef cursor = new BytesRef(packed.bytes, packed.offset, packed.length);
        byte[] nullBitmap = new byte[nullTrackingBytes];
        System.arraycopy(cursor.bytes, cursor.offset, nullBitmap, 0, nullTrackingBytes);
        cursor.offset += nullTrackingBytes;
        cursor.length -= nullTrackingBytes;

        BytesRef decodeScratch = new BytesRef();
        for (int g = 0; g < tailSpecs.size(); g++) {
            boolean isNull = (nullBitmap[g >> 3] & (1 << (g & 7))) != 0;
            if (isNull) {
                builders[g].appendNull();
                continue;
            }
            switch (tailSpecs.get(g).elementType()) {
                case BYTES_REF -> ((BytesRefBlock.Builder) builders[g]).appendBytesRef(ENCODER.decodeBytesRef(cursor, decodeScratch));
                case LONG -> ((LongBlock.Builder) builders[g]).appendLong(ENCODER.decodeLong(cursor));
                case INT -> ((IntBlock.Builder) builders[g]).appendInt(ENCODER.decodeInt(cursor));
                case DOUBLE -> ((DoubleBlock.Builder) builders[g]).appendDouble(ENCODER.decodeDouble(cursor));
                case BOOLEAN -> ((BooleanBlock.Builder) builders[g]).appendBoolean(ENCODER.decodeBoolean(cursor));
                default -> throw new IllegalStateException("unsupported tail element type [" + tailSpecs.get(g).elementType() + "]");
            }
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, Math.toIntExact(outerHash.size()));
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(outerHash.size());
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(outerHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public String toString() {
        return "PrefixBlockHash{prefix=["
            + prefixChannel
            + "], tail="
            + tailSpecs.size()
            + ", entries="
            + outerHash.size()
            + ", dictionarySize="
            + tailDictionary.ramBytesUsed()
            + "b, outerHashSize="
            + outerHash.ramBytesUsed()
            + "b, totalSize="
            + (tailDictionary.ramBytesUsed() + outerHash.ramBytesUsed())
            + "b}";
    }
}
