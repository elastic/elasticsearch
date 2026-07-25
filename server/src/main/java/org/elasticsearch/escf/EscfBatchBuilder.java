/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
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
 * Shared backend accumulator for ESCF column-major encoding. Owns the {@link SourceSchema}, a set
 * of per-partition {@link EscfColumnBuilder} lists, and a reusable {@link EscfRowBuffer}. Both the
 * x-content frontend ({@link EscfEncoder}) and the protobuf frontend ({@code MetricEscfConverter})
 * drive the same backend, so all optimization work on the column-building path benefits every
 * encoder.
 *
 * <p>Usage per row:
 * <ol>
 *   <li>{@link #beginRow()} — resets and returns the reusable {@link EscfRowBuffer}.</li>
 *   <li>Frontend writes fields into the buffer.</li>
 *   <li>{@link #commit(int)} — drains the buffer into the target partition's column builders and
 *       returns the row index within that partition.</li>
 * </ol>
 *
 * <p>Call {@link #buildPartition(int)} when all rows for a partition have been committed; the
 * resulting {@link EscfBatch} owns the column buffers and must be closed to release them.
 * Implements {@link Releasable}: close the builder when done to discard any uncommitted or
 * unbuilt column buffers.
 */
public final class EscfBatchBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final SourceSchema schema;
    private final Recycler<BytesRef> recycler;
    private final EscfRowBuffer rowBuffer;
    private Partition[] partitions;
    private String[] cachedPath;

    /**
     * Creates a builder using the non-recycling recycler (suitable for tests and low-volume paths).
     */
    public EscfBatchBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    /**
     * Creates a builder that uses {@code recycler} to back the column byte buffers, enabling
     * page-based memory reuse for high-throughput paths.
     */
    public EscfBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.schema = new SourceSchema();
        this.rowBuffer = new EscfRowBuffer(schema);
        this.partitions = new Partition[INITIAL_PARTITION_CAPACITY];
        this.cachedPath = new String[INITIAL_CAPACITY];
    }

    /**
     * Resets the shared {@link EscfRowBuffer} for a new row and returns it for the frontend to
     * populate. Must be called before {@link #commit(int)}.
     */
    public EscfRowBuffer beginRow() {
        rowBuffer.beginRow();
        return rowBuffer;
    }

    /**
     * Drains the staged row from the {@link EscfRowBuffer} into the column builders for
     * {@code partitionKey}. Returns the zero-based row index within the partition.
     *
     * @throws IllegalStateException if {@link #beginRow()} has not been called for the current row
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
     * {@link EscfBatch}. The batch owns the column buffers; close it to release them. After this
     * call, the partition's column builders are consumed and must not be used again.
     */
    public EscfBatch buildPartition(int partitionKey) {
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        final EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = partition.builders.get(c).finish(partition.docCount);
        }
        // Each column owns its recycler-backed buffers; the batch releases them all on close.
        return new EscfBatch(schema, partition.docCount, columns, Releasables.wrap(columns));
    }

    /**
     * Returns the number of rows committed to {@code partitionKey}, or 0 if no rows have been
     * committed to that partition.
     */
    public int docCount(int partitionKey) {
        Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        return partition == null ? 0 : partition.docCount;
    }

    /**
     * Returns {@code true} if at least one row has been committed to {@code partitionKey}.
     */
    public boolean hasPartition(int partitionKey) {
        return partitionKey < partitions.length && partitions[partitionKey] != null;
    }

    /**
     * Returns the full dot-separated path for leaf column {@code columnIndex}, caching the result
     * so repeated lookups are cheap.
     */
    public String columnPath(int columnIndex) {
        if (columnIndex >= cachedPath.length) {
            cachedPath = Arrays.copyOf(cachedPath, Integer.highestOneBit(columnIndex) << 1);
        }
        String path = cachedPath[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            cachedPath[columnIndex] = path;
        }
        return path;
    }

    /**
     * Discards all uncommitted and unbuilt column builders, releasing their recycler-backed
     * buffers. Safe to call after {@link #buildPartition} (the consumed builders' streams are
     * already empty).
     */
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
        if (partitionKey >= partitions.length) {
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
     * Ensures the partition has a column builder for every leaf in the schema. Any newly added
     * builder is backfilled with {@code addAbsent()} for all rows already committed to the
     * partition, since those rows did not contain the column.
     */
    private void ensurePartitionBuilders(Partition partition, int size) {
        while (partition.builders.size() < size) {
            EscfColumnBuilder builder = new EscfColumnBuilder(recycler);
            for (int i = 0; i < partition.docCount; i++) {
                builder.addAbsent();
            }
            partition.builders.add(builder);
        }
    }

    private static final class Partition {
        final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
        int docCount;
    }
}
