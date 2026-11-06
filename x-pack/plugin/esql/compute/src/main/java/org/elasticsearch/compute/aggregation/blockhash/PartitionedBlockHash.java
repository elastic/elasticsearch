/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.swisshash.SwissHashFactory;

import java.io.IOException;

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
     * Whether this hash writes and reads back one partition of its keys; a hash without a codec keeps its generations
     * resident, which is the first version's answer for time series.
     */
    public boolean supportsPartitionCodec() {
        return true;
    }

    /**
     * Writes one partition of keys this hash split, as {@link #readPartitionKeys} reads them back: this hash's own
     * wrapper state first, the nulls it folds into the keys, then the library's raw keys, so that a restored partition
     * is indistinguishable from a resident one to {@link #combinePartition}.
     */
    public abstract void writePartitionKeys(PartitionedHashKeys keys, int partition, StreamOutput out) throws IOException;

    /**
     * Reads one partition written by {@link #writePartitionKeys}, into keys holding that partition alone, charged to
     * {@code breaker} as a split's keys are.
     */
    public abstract PartitionedHashKeys readPartitionKeys(StreamInput in, int partition, CircuitBreaker breaker) throws IOException;
}
