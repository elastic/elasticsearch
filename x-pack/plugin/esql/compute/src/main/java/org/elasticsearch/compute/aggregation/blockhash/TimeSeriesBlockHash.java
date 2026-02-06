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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * An optimized block hash that receives two blocks: tsid and timestamp, which are sorted.
 * Since the incoming data is sorted, this block hash checks tsid ordinals to avoid redundant
 * hash lookups for consecutive positions with the same tsid and timestamp.
 * Delegates to a {@link BytesRefLongBlockHash} for the actual hashing.
 */
public final class TimeSeriesBlockHash extends BlockHash {

    private final int tsHashChannel;
    private final int timestampIntervalChannel;

    private final BytesRefLongBlockHash hash;

    private long lastBytesHashOrd;
    private long lastTimestamp;
    private int lastGroupId;

    public TimeSeriesBlockHash(int tsHashChannel, int timestampIntervalChannel, BlockFactory blockFactory) {
        super(blockFactory);
        this.tsHashChannel = tsHashChannel;
        this.timestampIntervalChannel = timestampIntervalChannel;
        this.hash = new BytesRefLongBlockHash(
            blockFactory,
            tsHashChannel,
            timestampIntervalChannel,
            false,
            Math.toIntExact(BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes())
        );
    }

    /**
     * Returns the underlying {@link BytesRefLongBlockHash} that this block hash delegates to.
     */
    public BytesRefLongBlockHash getHash() {
        return hash;
    }

    @Override
    public void close() {
        Releasables.close(hash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock tsidBlock = page.getBlock(tsHashChannel);
        var ordinalBlock = tsidBlock.asOrdinals();
        if (ordinalBlock == null) {
            // Non-ordinal block: fall back to the underlying hash
            hash.add(page, addInput);
            return;
        }
        var ordinalVector = ordinalBlock.asVector();
        if (ordinalVector == null) {
            // Not a vector: fall back to the underlying hash
            hash.add(page, addInput);
            return;
        }
        final BytesRefVector tsidDict = ordinalVector.getDictionaryVector();
        final IntVector tsidOrdinals = ordinalVector.getOrdinalsVector();
        final LongBlock timestampsBlock = page.getBlock(timestampIntervalChannel);
        LongVector timestampVector = timestampsBlock.asVector();
        if (timestampVector == null) {
            // Not a vector: fall back to the underlying hash
            hash.add(page, addInput);
            return;
        }
        final int positions = tsidOrdinals.getPositionCount();

        try (var ordsBuilder = blockFactory.newIntVectorBuilder(positions)) {
            final BytesRef spare = new BytesRef();
            // lastOrd is page-local since ordinals are relative to the page's dictionary
            int lastOrd = -1;
            for (int i = 0; i < positions; i++) {
                final int newOrd = tsidOrdinals.getInt(i);
                final long timestamp = timestampVector.getLong(i);
                if (newOrd == lastOrd && timestamp == lastTimestamp) {
                    // Same tsid ordinal and same timestamp as previous position: reuse the group id
                    ordsBuilder.appendInt(lastGroupId);
                } else {
                    if (newOrd != lastOrd) {
                        // Ordinal changed: hash the new BytesRef to get its bytes hash ordinal
                        lastBytesHashOrd = hash.addBytesRef(tsidDict.getBytesRef(newOrd, spare));
                        lastOrd = newOrd;
                    }
                    // Add (bytesHashOrd, timestamp) to the final hash
                    lastGroupId = Math.toIntExact(hashOrdToGroup(hash.addGroup(lastBytesHashOrd, timestamp)));
                    lastTimestamp = timestamp;
                    ordsBuilder.appendInt(lastGroupId);
                }
            }
            try (var ords = ordsBuilder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        return hash.getKeys();
    }

    @Override
    public IntVector nonEmpty() {
        return hash.nonEmpty();
    }

    @Override
    public int numKeys() {
        return hash.numKeys();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return hash.seenGroupIds(bigArrays);
    }

    public String toString() {
        return "TimeSeriesBlockHash{keys=[BytesRefKey[channel="
            + tsHashChannel
            + "], LongKey[channel="
            + timestampIntervalChannel
            + "]], entries="
            + hash.numKeys()
            + "b}";
    }
}
