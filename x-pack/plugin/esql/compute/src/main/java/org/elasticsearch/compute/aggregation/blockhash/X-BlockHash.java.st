/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
$endif$
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
$if(BytesRef)$
import org.elasticsearch.common.util.BytesRefArray;
$endif$
import org.elasticsearch.common.util.$Hash$;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
$if(BytesRef)$
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
$elseif(double)$
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
$elseif(int)$
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
$elseif(long)$
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
$endif$
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe$Type$;

$if(BytesRef)$
import java.io.IOException;

$else$
import java.util.BitSet;

$endif$
/**
 * Maps a {@link $Type$Block} column to group ids.
 */
final class $Type$BlockHash extends BlockHash {
    private final int channel;
    final $Hash$ hash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    $Type$BlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.hash = new $Hash$(1, blockFactory.bigArrays());
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            seenNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
            return;
        }
        $Type$Block castBlock = ($Type$Block) block;
        $Type$Vector vector = castBlock.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(castBlock)) {
                addInput.add(0, groupIds);
            }
            return;
        }
        try (IntVector groupIds = add(vector)) {
            addInput.add(0, groupIds);
        }
    }

    IntVector add($Type$Vector vector) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
$if(double)$
                long v = Double.doubleToLongBits(vector.getDouble(i));
$elseif(BytesRef)$
                BytesRef v = vector.getBytesRef(i, scratch);
$else$
                $type$ v = vector.get$Type$(i);
$endif$
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
            }
            return builder.build();
        }
    }

    IntBlock add($Type$Block block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupe$Type$(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public $Type$Block[] getKeys() {
$if(BytesRef)$
        /*
         * Create an un-owned copy of the data so we can close our BytesRefHash
         * without and still read from the block.
         */
        // TODO replace with takeBytesRefsOwnership ?!
        if (seenNull) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(Math.toIntExact(hash.size() + 1))) {
                builder.appendNull();
                BytesRef spare = new BytesRef();
                for (long i = 0; i < hash.size(); i++) {
                    builder.appendBytesRef(hash.get(i, spare));
                }
                return new BytesRefBlock[] { builder.build() };
            }
        }

        final int size = Math.toIntExact(hash.size());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            hash.getBytesRefs().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new BytesRefBlock[] {
                    blockFactory.newBytesRefArrayVector(new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE), size).asBlock() };
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
$else$
        if (seenNull) {
            final int size = Math.toIntExact(hash.size() + 1);
            final $type$[] keys = new $type$[size];
            for (int i = 1; i < size; i++) {
$if(int)$
                keys[i] = (int) hash.get(i - 1);
$elseif(double)$
                keys[i] = Double.longBitsToDouble(hash.get(i - 1));
$elseif(long)$
                keys[i] = hash.get(i - 1);
$else$
                syntax error because we didn't cover this case
$endif$
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new $Type$Block[] {
                blockFactory.new$Type$ArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING) };
        }
        final int size = Math.toIntExact(hash.size());
        final $type$[] keys = new $type$[size];
        for (int i = 0; i < size; i++) {
$if(int)$
            keys[i] = (int) hash.get(i);
$elseif(double)$
            keys[i] = Double.longBitsToDouble(hash.get(i));
$elseif(long)$
            keys[i] = hash.get(i);
$else$
            syntax error because we didn't cover this case
$endif$
        }
        return new $Type$Block[] { blockFactory.new$Type$ArrayVector(keys, keys.length).asBlock() };
$endif$
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("$Type$BlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
$if(BytesRef)$
        b.append(", size=").append(ByteSizeValue.ofBytes(hash.ramBytesUsed()));
$endif$
        b.append(", seenNull=").append(seenNull);
        return b.append('}').toString();
    }
}
