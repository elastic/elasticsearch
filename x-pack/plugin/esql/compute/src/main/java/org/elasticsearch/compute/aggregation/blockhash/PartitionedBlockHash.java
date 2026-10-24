/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.swisshash.SwissHashFactory;

import java.util.List;

/**
 * An extension of {@link BlockHash} to support partitioning.
 * This class should be removed once all {@link BlockHash} supports partitioning.
 */
public abstract class PartitionedBlockHash extends BlockHash implements PartitionedHashTable {
    protected PartitionedBlockHash(BlockFactory blockFactory) {
        super(blockFactory);
    }

    public static boolean supportPartitioning() {
        return SwissHashFactory.getInstance() != null;
    }

    /**
     * Estimates the bytes used by the active keys for deciding when to partition.
     */
    public abstract long estimatedBytesForPartitioning();

    /**
     * Estimates the bytes used by the active keys in a {@link BytesRefArray}
     */
    protected static long estimatedKeyBytes(BytesRefArray bytesRefs) {
        final long offsetBytes = bytesRefs.fixedLength() >= 0 ? 0 : bytesRefs.size() * Integer.BYTES;
        return bytesRefs.totalBytes() + offsetBytes;
    }

    public abstract void clear();

    /**
     * Builds a hash for a lookup table keyed on {@code groups}, picking the same hash as {@link BlockHash#build}
     * picks for grouping by them. The exception is two or three keyword keys: their hashes can't {@link #lookup},
     * so they get a {@link PackedValuesBlockHash}. The hash's {@link #lookup} doesn't modify it, so once built it
     * can be probed from several threads.
     */
    public static BlockHash buildForLookup(List<GroupSpec> groups, BlockFactory blockFactory) {
        int emitBatchSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        if ((groups.size() == 2 || groups.size() == 3) && groups.stream().allMatch(g -> g.elementType() == ElementType.BYTES_REF)) {
            return new PackedValuesBlockHash(groups, blockFactory, emitBatchSize);
        }
        return BlockHash.build(groups, blockFactory, emitBatchSize, false);
    }

    /**
     * Whether {@link #buildForLookup} builds a {@link PartitionedBlockHash} for {@code groups}, which can be split and
     * recombined into partitions probed with {@link #lookupPartitioned}. Every multi-key hash supports that, but only
     * the int, long and keyword single-key hashes do.
     */
    public static boolean supportsPartitionedLookup(List<GroupSpec> groups) {
        if (supportPartitioning() == false) {
            return false;
        }
        if (groups.size() > 1) {
            return true;
        }
        return switch (groups.get(0).elementType()) {
            case INT, LONG, BYTES_REF -> true;
            case NULL, BOOLEAN, DOUBLE, DOUBLE_RANGE -> false;
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + groups.get(0).elementType() + "]");
        };
    }

    /**
     * Builds the {@link #buildForLookup} hash of {@code groups}, which must {@link #supportsPartitionedLookup}.
     */
    public static PartitionedBlockHash buildPartitionedForLookup(List<GroupSpec> groups, BlockFactory blockFactory) {
        BlockHash hash = buildForLookup(groups, blockFactory);
        if (hash instanceof PartitionedBlockHash partitioned) {
            return partitioned;
        }
        hash.close();
        throw new IllegalArgumentException("lookups on " + groups + " don't support partitioning");
    }

    /**
     * Looks up the keys of {@code page} in a table split into {@link PartitionedHashTable#NUM_PARTITIONS} hashes, where
     * {@code partitions[p]} holds exactly the keys that {@link #splitPartition} puts in partition {@code p}. For each
     * position returns {@code groupBase[p]} plus the key's group id in {@code partitions[p]}, or null if no partition
     * holds the key. Like {@link #lookup} it doesn't modify the hashes. Every hash must come from
     * {@link #buildForLookup} with the same {@link GroupSpec}s.
     */
    public static ReleasableIterator<IntBlock> lookupPartitioned(
        Page page,
        ByteSizeValue targetBlockSize,
        PartitionedBlockHash[] partitions,
        int[] groupBase
    ) {
        assert partitions.length == PartitionedHashTable.NUM_PARTITIONS : partitions.length;
        assert groupBase.length >= PartitionedHashTable.NUM_PARTITIONS : groupBase.length;
        return partitions[0].lookupInPartitions(page, targetBlockSize, partitions, groupBase);
    }

    /**
     * Implements {@link #lookupPartitioned}. {@code partitions} are all of this hash's type.
     */
    protected ReleasableIterator<IntBlock> lookupInPartitions(
        Page page,
        ByteSizeValue targetBlockSize,
        PartitionedBlockHash[] partitions,
        int[] groupBase
    ) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioned lookups");
    }
}
