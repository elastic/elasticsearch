/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import com.google.protobuf.ByteString;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared infrastructure for building a single-partition {@link EscfBatch} from OTLP data whose document
 * layout is known upfront. A domain-specific alternative to the generic
 * {@link org.elasticsearch.escf.EscfBatchBuilder}: rather than discovering the schema one field at a time via
 * name-based {@code startObject}/{@code endObject} calls, subclasses declare their fixed object and leaf
 * fields once (see {@link #objectField}, {@link #leafField}, {@link #lazyLeafField}) and then write values by
 * their resolved column index. This class carries no knowledge of any particular schema.
 *
 * <p>There is no intermediate row buffer: every {@code xxxField(colIdx, value)} writer writes straight into
 * the target column's {@link EscfColumnBuilder}. A column that a given row skips simply falls behind;
 * {@link #catchUp} bulk-fills the gap with {@link EscfColumnBuilder#addAbsents} the next time that column is
 * written (or, for a column nothing ever writes again, once at {@link #build()}), so the per-row cost scales
 * with the number of fields the row actually writes rather than with the total schema size. Writing the same
 * column twice in one row shows up as that column being ahead of the row being written and is rejected with
 * an {@link IllegalArgumentException}; fields whose name comes from the payload should check
 * {@link #hasValue} first.
 *
 * <p>Usage per row: write fields, then call {@link #commitRow()}. Call {@link #build()} once every row has
 * been committed; the returned {@link EscfBatch} owns the column buffers and must be closed to release them.
 *
 * <p>Because writes land directly in the column builders, a row that fails partway through (the caller throws
 * before calling {@link #commitRow()}) cannot be un-written: whichever columns it already touched are left one
 * row ahead of {@link #docCount()}, so the whole builder must be discarded. TODO: add an {@code abortRow()}
 * that tombstones the partially-written row instead, so a single failed row doesn't force discarding the rest
 * of an otherwise-good batch.
 *
 * <p><b>Extension point for bulk writes:</b> a caller that knows a value is shared by many consecutive rows
 * (e.g. a resource attribute repeated for every data-point group under the same resource) could write it once
 * for a whole row range via the column's positional {@code EscfColumnBuilder.setX(row, value)} API; wiring
 * that up is left for when a caller needs it.
 */
public abstract class AbstractOTLPBatchBuilder implements Releasable {

    private static final int INITIAL_LEAF_CAPACITY = 32;

    /** Parent index of top-level fields. */
    protected static final int ROOT = 0;

    /** Sentinel for a {@link #lazyLeafField} that hasn't been registered on this builder yet. */
    protected static final int NOT_REGISTERED = -1;

    private final SourceSchema schema = new SourceSchema();
    private final Recycler<BytesRef> recycler;
    private final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_LEAF_CAPACITY);

    private int docCount;
    private boolean built;

    protected AbstractOTLPBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    // -------------------------------------------------------------------------
    // Schema declaration
    // -------------------------------------------------------------------------

    /** Registers (or reuses) the object field {@code name} under {@code parentIdx} and returns its index. */
    protected final int objectField(String name, int parentIdx) {
        return schema.appendNonLeaf(name, parentIdx);
    }

    /** Registers (or reuses) the leaf field {@code name} under {@code parentIdx} and returns its column index. */
    protected final int leafField(String name, int parentIdx) {
        return schema.appendLeaf(name, parentIdx);
    }

    /**
     * Resolves a leaf field that is registered on first use. {@code cached} is the field's current value
     * ({@link #NOT_REGISTERED} if never written on this builder); callers store the result back so later
     * calls skip the schema lookup. Use for optional fields that are absent far more often than not, so
     * batches that never write them don't carry an all-absent column.
     */
    protected final int lazyLeafField(int cached, String name, int parentIdx) {
        return cached != NOT_REGISTERED ? cached : schema.appendLeaf(name, parentIdx);
    }

    // -------------------------------------------------------------------------
    // Row lifecycle
    // -------------------------------------------------------------------------

    /**
     * Records the current row as complete and returns its zero-based row index. Must be called exactly once
     * per row, after every field write for that row has succeeded. There is no {@code beginRow()}: writes go
     * directly into the column builders, so there's no per-row state to set up or flush.
     */
    public final int commitRow() {
        return docCount++;
    }

    /** Returns the number of rows committed so far. */
    public final int docCount() {
        return docCount;
    }

    /**
     * Finalizes all column builders and returns the resulting {@link EscfBatch}. The batch owns the column
     * buffers; close it to release them. Calling this twice is an error.
     */
    public final EscfBatch build() {
        if (built) {
            throw new IllegalStateException("build() already called");
        }
        int leafCount = schema.leafCount();
        EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = catchUp(c, docCount).finish(docCount);
        }
        built = true;
        // Each column owns its recycler-backed buffers; the batch releases them all on close.
        return EscfBatch.of(schema, docCount, columns, Releasables.wrap(columns));
    }

    /** Releases all unbuilt column builders, freeing their recycler-backed buffers. */
    @Override
    public void close() {
        if (built == false) {
            Releasables.close(builders);
        }
    }

    // -------------------------------------------------------------------------
    // Field writers, addressed by column index
    // -------------------------------------------------------------------------

    /** Writes a long value at {@code colIdx}. */
    public final void longField(int colIdx, long value) {
        catchUp(colIdx, docCount).addLong(value);
    }

    /** Writes a double value at {@code colIdx}. */
    public final void doubleField(int colIdx, double value) {
        catchUp(colIdx, docCount).addDouble(value);
    }

    /** Writes a boolean value at {@code colIdx}. */
    public final void booleanField(int colIdx, boolean value) {
        catchUp(colIdx, docCount).addBoolean(value);
    }

    /** Writes a UTF-8 string value at {@code colIdx} from a raw byte slice. */
    public final void stringField(int colIdx, byte[] bytes, int offset, int length) {
        catchUp(colIdx, docCount).addString(new XContentString.UTF8Bytes(bytes, offset, length));
    }

    public final void stringField(int colIdx, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        stringField(colIdx, bytes, 0, bytes.length);
    }

    public final void stringField(int colIdx, ByteString value) {
        byte[] bytes = value.toByteArray();
        stringField(colIdx, bytes, 0, bytes.length);
    }

    /**
     * Returns {@code true} if {@code colIdx} has already been written as a leaf in the current (uncommitted) row.
     * Callers writing fields whose name comes from the payload (e.g. attribute keys) use this to skip
     * duplicates, which a column can't represent.
     */
    public final boolean hasValue(int colIdx) {
        return colIdx < builders.size() && builders.get(colIdx).nextAppendRow() > docCount;
    }

    /**
     * Returns {@code colIdx}'s column builder, bulk-filling it with absents up to (but not including)
     * {@code targetRow} if it has fallen behind. If the builder is already ahead of {@code targetRow}, the
     * caller is writing the same column twice in one row, which is not representable.
     */
    private EscfColumnBuilder catchUp(int colIdx, int targetRow) {
        while (builders.size() <= colIdx) {
            builders.add(new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler));
        }
        EscfColumnBuilder builder = builders.get(colIdx);
        int gap = targetRow - builder.nextAppendRow();
        if (gap < 0) {
            throw new IllegalArgumentException("Duplicate field [" + schema.getFullPath(colIdx) + "]");
        }
        if (gap > 0) {
            builder.addAbsents(gap);
        }
        return builder;
    }
}
