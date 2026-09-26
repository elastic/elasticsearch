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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Shared backend for ESCF column-major encoding. Owns a {@link SourceSchema} and per-column
 * {@link EscfColumnBuilder}s. Field values are written directly into the column builders — there
 * is no intermediate row buffer.
 *
 * <p>Usage per document: call {@link #beginRow()} to start a new row, write field values via the
 * field-writer methods, then call {@link #finishRow()} to absent-fill untouched columns and obtain
 * the zero-based row index. On a parse failure mid-row, call {@link #abortRow()} instead; the
 * partial row is completed with absent values and becomes an orphan row that the scatter step will
 * route to the discard partition.
 *
 * <p>Call {@link #build()} once all documents are encoded to obtain the finished {@link EscfBatch}.
 * The batch owns the column buffers; close it to release them.
 */
public final class EscfBatchBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARENT_STACK_DEPTH = 8;

    private final SourceSchema schema;
    private final Recycler<BytesRef> recycler;
    private final List<EscfColumnBuilder> builders;

    /** Tracks which column indices have been written in the current row. Cleared by {@link #beginRow()}. */
    private FixedBitSet touched;

    /** Stack of non-leaf (object) indices for tracking the current nesting context. */
    private int[] parentStack;
    private int parentDepth;

    private int docCount;

    /** Creates a builder using the non-recycling recycler (suitable for tests). */
    public EscfBatchBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    /** Creates a builder using {@code recycler} to back column byte buffers for high-throughput paths. */
    public EscfBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.schema = new SourceSchema();
        this.builders = new ArrayList<>(INITIAL_CAPACITY);
        this.touched = new FixedBitSet(Math.max(INITIAL_CAPACITY, 64));
        this.parentStack = new int[INITIAL_PARENT_STACK_DEPTH];
        // parentStack[0] = 0 (root) is already set by zero-initialization
    }

    /**
     * Resets state for a new document row. Must be called before any field writes for the row.
     */
    public void beginRow() {
        touched.clear();
        parentDepth = 0;
    }

    /**
     * Absent-fills any columns not touched during this row, increments the row count, and returns
     * the zero-based row index. Must be called after all field writes for the row succeed.
     */
    public int finishRow() {
        absentFillUntouched();
        int rowIndex = docCount;
        docCount++;
        return rowIndex;
    }

    /**
     * Called when parsing fails partway through a row (e.g. malformed JSON in the SIMD path). Absent-fills
     * any un-written columns and increments the row count, producing an orphan row. The orphan row
     * index is never returned to the caller; the caller should re-parse the document and produce a
     * valid row via a fresh {@link #beginRow()} / {@link #finishRow()} cycle.
     *
     * <p>The orphan row contains whatever partial values the failed parse wrote, plus absent-fills
     * for everything else. It is well-formed (every column builder holds exactly {@code docCount}
     * values after this call), and will be routed to the discard partition during scatter.
     *
     * <p>Columns that received a partial value before the failure may be promoted to
     * {@link EscfColumnKind#UNION} by the absent-fill — this is lossless and only slightly less compact.
     */
    public void abortRow() {
        absentFillUntouched();
        docCount++;
    }

    /** Descends into a named nested object, pushing the parent context. Pair with {@link #endObject()}. */
    public void startObject(String name) {
        int nonLeafIdx = schema.appendNonLeaf(name, parentStack[parentDepth]);
        parentDepth++;
        ensureParentStackCapacity();
        parentStack[parentDepth] = nonLeafIdx;
    }

    /** Exits the current nested object, restoring the parent context. */
    public void endObject() {
        assert parentDepth > 0 : "endObject called without matching startObject";
        parentDepth--;
    }

    /** Encodes an empty object as a zero-byte {@code KEY_VALUE} leaf, keeping it distinct from absent. */
    public void emptyObject(String name) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addKeyValue(BytesRef.EMPTY_BYTES);
    }

    /** Writes a long field. */
    public void longField(String name, long value) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addLong(value);
    }

    /** Writes a double field. */
    public void doubleField(String name, double value) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addDouble(value);
    }

    /** Writes a UTF-8 string value. */
    public void stringField(String name, XContentString.UTF8Bytes value) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addString(value);
    }

    /** Writes a UTF-8 string value from a raw byte slice. */
    public void stringField(String name, byte[] bytes, int offset, int length) {
        stringField(name, new XContentString.UTF8Bytes(bytes, offset, length));
    }

    /** Writes a boolean value. */
    public void booleanField(String name, boolean value) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addBoolean(value);
    }

    /** Writes an explicit null. */
    public void nullField(String name) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addNull();
    }

    /**
     * Writes a packed inline array ({@code SourceValueType.FIXED_ARRAY} or
     * {@code SourceValueType.UNION_ARRAY}).
     */
    public void arrayField(String name, byte arrayType, byte[] packed) {
        int colIdx = addLeaf(name);
        ensureBuilder(colIdx).addArray(arrayType, packed);
    }

    /** Returns the number of completed rows (each call to {@link #finishRow()} or {@link #abortRow()}). */
    public int docCount() {
        return docCount;
    }

    /**
     * Finalizes all column builders and returns the resulting {@link EscfBatch}. The batch owns the
     * column buffers; close it to release them. After this call, the builder is effectively empty;
     * calling {@link #close()} is still safe but a no-op for the built columns.
     */
    public EscfBatch build() {
        int leafCount = schema.leafCount();
        // Ensure builders exist for every schema leaf (handles columns that only ever got absent-fills).
        while (builders.size() < leafCount) {
            EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler);
            b.addAbsents(docCount);
            builders.add(b);
        }
        EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = builders.get(c).finish(docCount);
        }
        builders.clear();
        return new EscfBatch(schema, docCount, columns, Releasables.wrap(columns));
    }

    /** Discards all uncommitted column builders, releasing their recycler-backed buffers. */
    @Override
    public void close() {
        Releasables.close(builders);
        builders.clear();
    }

    private int addLeaf(String name) {
        int colIdx = schema.appendLeaf(name, parentStack[parentDepth]);
        if (colIdx >= touched.length()) {
            touched = FixedBitSet.ensureCapacity(touched, ArrayUtil.oversize(colIdx + 1, 1));
        }
        if (touched.getAndSet(colIdx)) {
            throw new IllegalArgumentException("Duplicate field [" + name + "]");
        }
        return colIdx;
    }

    /**
     * Returns the builder for {@code colIdx}, creating it and backfilling absent rows if needed.
     */
    private EscfColumnBuilder ensureBuilder(int colIdx) {
        while (builders.size() <= colIdx) {
            EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler);
            b.addAbsents(docCount);
            builders.add(b);
        }
        return builders.get(colIdx);
    }

    /**
     * Absent-fills all columns not touched in the current row. Called by both {@link #finishRow()}
     * and {@link #abortRow()} to ensure every column builder holds exactly {@code docCount + 1}
     * values after the call (the caller increments {@code docCount}).
     */
    private void absentFillUntouched() {
        int leafCount = schema.leafCount();
        for (int c = touched.nextClearBit(0); c < leafCount; c = touched.nextClearBit(c + 1)) {
            ensureBuilder(c).addAbsent();
        }
    }

    private void ensureParentStackCapacity() {
        if (parentDepth < parentStack.length) {
            return;
        }
        parentStack = Arrays.copyOf(parentStack, parentStack.length * 2);
    }
}
