/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Shared backend for ESCF column-major encoding. Owns a {@link SourceSchema}, per-partition
 * {@link EscfColumnBuilder} lists, and a reusable {@link EscfRowBuffer}.
 *
 * <p>Usage per row: call {@link #beginRow()} to get the row buffer, populate it, call
 * {@link EscfRowBuffer#finishRow()} to mark the row complete, then {@link #commit(int)} to flush
 * into the partition's column builders. Call
 * {@link #buildPartition(int)} once all rows for a partition are committed; the resulting
 * {@link EscfBatch} owns the column buffers and must be closed to release them.
 *
 * <p>Partition keys must be non-negative integers (e.g., shard indices).
 */
public final class EscfBatchBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final SourceSchema schema;
    private final Recycler<BytesRef> recycler;
    private final EscfRowBuffer rowBuffer;
    private Partition[] partitions;
    private String[] cachedPath;

    /** Creates a builder using the non-recycling recycler (suitable for tests). */
    public EscfBatchBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    /** Creates a builder using {@code recycler} to back column byte buffers for high-throughput paths. */
    public EscfBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.schema = new SourceSchema();
        this.rowBuffer = new EscfRowBuffer(schema);
        this.partitions = new Partition[INITIAL_PARTITION_CAPACITY];
        this.cachedPath = new String[INITIAL_CAPACITY];
    }

    /**
     * Resets the row buffer for a new row and returns it. Populate fields, then call
     * {@link EscfRowBuffer#finishRow()} before {@link #commit(int)}.
     */
    public EscfRowBuffer beginRow() {
        rowBuffer.beginRow();
        return rowBuffer;
    }

    /**
     * Drains the staged row into {@code partitionKey}'s column builders and returns its
     * zero-based row index within that partition.
     *
     * @throws IllegalStateException if {@link EscfRowBuffer#finishRow()} was not called for the current row
     */
    public int commit(int partitionKey) {
        if (rowBuffer.isStarted() == false) {
            throw new IllegalStateException("commit called without a staged row");
        }
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        for (int c = 0; c < leafCount; c++) {
            drainScratchValue(partition.builders.get(c), c);
        }
        final int rowIndex = partition.docCount;
        partition.docCount++;
        rowBuffer.rowStarted = false;
        return rowIndex;
    }

    /**
     * Finalizes all column builders for {@code partitionKey} and returns the resulting
     * {@link EscfBatch}. The batch owns the column buffers; close it to release them.
     * The partition entry is cleared; calling this twice for the same key is an error.
     */
    public EscfBatch buildPartition(int partitionKey) {
        final Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        if (partition == null) {
            throw new IllegalStateException("no committed rows for partition [" + partitionKey + "] (already built or never written)");
        }
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        final EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = partition.builders.get(c).finish(partition.docCount);
        }
        // Clear the partition so close() skips it; a second call throws above.
        partitions[partitionKey] = null;
        // Each column owns its recycler-backed buffers; the batch releases them all on close.
        return new EscfBatch(schema, partition.docCount, columns, Releasables.wrap(columns));
    }

    /** Returns the number of rows committed to {@code partitionKey}, or 0 if none. */
    public int docCount(int partitionKey) {
        Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        return partition == null ? 0 : partition.docCount;
    }

    /** Returns {@code true} if at least one row has been committed to {@code partitionKey}. */
    public boolean hasPartition(int partitionKey) {
        return partitionKey < partitions.length && partitions[partitionKey] != null;
    }

    /** Returns the full dot-separated path for leaf column {@code columnIndex}, with caching. */
    public String columnPath(int columnIndex) {
        if (columnIndex >= cachedPath.length) {
            cachedPath = Arrays.copyOf(cachedPath, ArrayUtil.oversize(columnIndex + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF));
        }
        String path = cachedPath[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            cachedPath[columnIndex] = path;
        }
        return path;
    }

    /** Discards all uncommitted and unbuilt column builders, releasing their recycler-backed buffers. */
    @Override
    public void close() {
        for (Partition partition : partitions) {
            if (partition != null) {
                for (EscfColumnBuilder builder : partition.builders) {
                    builder.discard();
                }
            }
        }
        Arrays.fill(partitions, null);
    }

    private void drainScratchValue(EscfColumnBuilder builder, int col) {
        final byte type = rowBuffer.scratchType(col);
        switch (type) {
            case SourceValueType.ABSENT -> builder.addAbsent();
            case SourceValueType.NULL -> builder.addNull();
            case SourceValueType.TRUE -> builder.addBoolean(true);
            case SourceValueType.FALSE -> builder.addBoolean(false);
            case SourceValueType.INT, SourceValueType.LONG -> builder.addLong(rowBuffer.scratchNumeric(col));
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> builder.addDouble(Double.longBitsToDouble(rowBuffer.scratchNumeric(col)));
            case SourceValueType.STRING -> builder.addString((XContentString.UTF8Bytes) rowBuffer.scratchVar(col));
            case SourceValueType.FIXED_ARRAY, SourceValueType.UNION_ARRAY -> builder.addArray(type, (byte[]) rowBuffer.scratchVar(col));
            case SourceValueType.KEY_VALUE -> builder.addKeyValue((byte[]) rowBuffer.scratchVar(col));
            default -> throw new IllegalStateException("unexpected scratch type [" + SourceValueType.name(type) + "]");
        }
    }

    private Partition getOrCreatePartition(int partitionKey) {
        assert partitionKey >= 0 : "partition key must be a non-negative shard index, got " + partitionKey;
        if (partitionKey >= partitions.length) {
            // Shard indices are bounded well below Integer.MAX_VALUE, so newCap never overflows.
            int newCap = partitions.length;
            while (partitionKey >= newCap) {
                newCap <<= 1;
            }
            partitions = Arrays.copyOf(partitions, newCap);
        }
        Partition partition = partitions[partitionKey];
        if (partition == null) {
            partition = new Partition();
            partitions[partitionKey] = partition;
        }
        return partition;
    }

    /**
     * Ensures the partition has a builder for every leaf. Newly added builders are backfilled with
     * absent rows for prior commits via {@link EscfColumnBuilder#addAbsents(int)}.
     */
    private void ensurePartitionBuilders(Partition partition, int size) {
        while (partition.builders.size() < size) {
            EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler);
            builder.addAbsents(partition.docCount);
            partition.builders.add(builder);
        }
    }

    private static final class Partition {
        final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
        int docCount;
    }
}
