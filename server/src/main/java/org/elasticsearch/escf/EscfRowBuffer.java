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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;

/**
 * A mutable, single-row staging buffer for ESCF column-major encoding — the write-side dual of
 * {@link org.elasticsearch.sourcebatch.SourceRow}.
 *
 * <p>Both types index one row by leaf-column against a {@link SourceSchema}, but they serve
 * opposite roles: {@link org.elasticsearch.sourcebatch.SourceRow} (implemented by {@code EscfRow})
 * is an immutable <em>read</em> view over finished column vectors, whereas {@code EscfRowBuffer} is
 * a mutable <em>write</em> buffer of staged values that feed {@link EscfColumnBuilder}s when
 * committed via {@link EscfBatchBuilder#commit}. It deliberately does <em>not</em> implement
 * {@code SourceRow}: there are no finished columns, the buffer is partial and mutable, and it
 * grows the schema as fields are encountered.
 *
 * <p>Usage:
 * <ol>
 *   <li>Call {@link #beginRow()} to reset the buffer for a new row (or call
 *       {@link EscfBatchBuilder#beginRow()}, which does this and returns the buffer).</li>
 *   <li>Populate fields using {@link #startObject}/{@link #endObject} and the leaf writers
 *       ({@link #longField}, {@link #stringField}, etc.).</li>
 *   <li>Call {@link EscfBatchBuilder#commit} to drain the staged values into column builders.</li>
 * </ol>
 *
 * <p>Both the x-content and protobuf frontends write into this buffer; the shared
 * {@link EscfBatchBuilder} backend drains it. The staged-value model uses
 * {@link SourceValueType} constants and is thus format-neutral. If EIRF or other formats ever need
 * this, it could move to {@code org.elasticsearch.sourcebatch} as a {@code SourceRowBuffer}.
 */
public final class EscfRowBuffer {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARENT_STACK_DEPTH = 8;

    private final SourceSchema schema;

    private byte[] scratchType;
    private long[] scratchNumeric;
    private Object[] scratchVar;
    private FixedBitSet columnsSet;

    private int[] parentStack;
    private int parentDepth;

    /**
     * Whether {@link #beginRow()} has been called for the current row and
     * {@link EscfBatchBuilder#commit} has not yet been called.
     * Package-private so {@link EscfBatchBuilder#commit} can reset it after draining.
     */
    boolean rowStarted;

    EscfRowBuffer(SourceSchema schema) {
        this.schema = schema;
        this.scratchType = new byte[INITIAL_CAPACITY];
        this.scratchNumeric = new long[INITIAL_CAPACITY];
        this.scratchVar = new Object[INITIAL_CAPACITY];
        this.columnsSet = new FixedBitSet(Math.max(INITIAL_CAPACITY, 64));
        this.parentStack = new int[INITIAL_PARENT_STACK_DEPTH];
        // parentStack[0] = 0 (root) is already set by zero-initialization
    }

    /**
     * Resets the buffer for a new row. Clears scratch type and var slots for all known columns,
     * resets the duplicate-detection bitset, and positions the parent context at the root.
     * Must be called before any field writes.
     */
    public void beginRow() {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratchType, 0, Math.min(columnCountBefore, scratchType.length), (byte) 0);
        Arrays.fill(scratchVar, 0, Math.min(columnCountBefore, scratchVar.length), null);
        columnsSet.clear();
        parentDepth = 0;
        rowStarted = true;
    }

    /**
     * Descends into the named nested object, pushing the current parent context. The non-leaf
     * index is resolved via the shared schema (idempotent). Must be paired with
     * {@link #endObject()}.
     */
    public void startObject(String name) {
        int nonLeafIdx = schema.appendNonLeaf(name, parentStack[parentDepth]);
        parentDepth++;
        ensureParentStackCapacity();
        parentStack[parentDepth] = nonLeafIdx;
    }

    /**
     * Exits the current nested object, restoring the parent context.
     */
    public void endObject() {
        parentDepth--;
    }

    /**
     * Encodes an empty object ({@code {}}) as a zero-byte {@code KEY_VALUE} leaf, keeping it
     * distinguishable from an absent field. Returns the leaf column index.
     */
    public int emptyObject(String name) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.KEY_VALUE;
        scratchVar[colIdx] = BytesRef.EMPTY_BYTES;
        return colIdx;
    }

    /**
     * Stages a long value for {@code name} under the current parent. Uses {@code INT} encoding if
     * the value fits in an {@code int} range, {@code LONG} otherwise. Returns the leaf column index.
     */
    public int longField(String name, long value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) ? SourceValueType.INT : SourceValueType.LONG;
        scratchNumeric[colIdx] = value;
        return colIdx;
    }

    /**
     * Stages a double value for {@code name} under the current parent. Uses {@code FLOAT} encoding
     * if the value round-trips exactly through {@code float}, {@code DOUBLE} otherwise. In both
     * cases the value is stored as {@code Double.doubleToRawLongBits(value)} in the numeric slot.
     * Returns the leaf column index.
     */
    public int doubleField(String name, double value) {
        int colIdx = addLeaf(name);
        float fval = (float) value;
        scratchType[colIdx] = ((double) fval == value) ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
        scratchNumeric[colIdx] = Double.doubleToRawLongBits(value);
        return colIdx;
    }

    /**
     * Stages a UTF-8 string value for {@code name} under the current parent. Returns the leaf
     * column index.
     */
    public int stringField(String name, XContentString.UTF8Bytes value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.STRING;
        scratchVar[colIdx] = value;
        return colIdx;
    }

    /**
     * Stages a UTF-8 string value for {@code name} from a raw byte slice. Convenience overload
     * that wraps the slice into a {@link XContentString.UTF8Bytes}. Returns the leaf column index.
     */
    public int stringField(String name, byte[] bytes, int offset, int length) {
        return stringField(name, new XContentString.UTF8Bytes(bytes, offset, length));
    }

    /**
     * Stages a boolean value for {@code name} under the current parent. Returns the leaf column
     * index.
     */
    public int booleanField(String name, boolean value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = value ? SourceValueType.TRUE : SourceValueType.FALSE;
        return colIdx;
    }

    /**
     * Stages an explicit JSON null for {@code name} under the current parent. Returns the leaf
     * column index.
     */
    public int nullField(String name) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.NULL;
        return colIdx;
    }

    /**
     * Stages an inline array payload for {@code name} under the current parent. {@code arrayType}
     * must be {@link SourceValueType#FIXED_ARRAY} or {@link SourceValueType#UNION_ARRAY};
     * {@code packed} is the corresponding byte payload produced by
     * {@link org.elasticsearch.sourcebatch.SourceBatchEncodeHelper}. Returns the leaf column index.
     */
    public int arrayField(String name, byte arrayType, byte[] packed) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = arrayType;
        scratchVar[colIdx] = packed;
        return colIdx;
    }

    // ── Package-private read-back — used only by EscfBatchBuilder to drain into column builders ──

    byte scratchType(int col) {
        return scratchType[col];
    }

    long scratchNumeric(int col) {
        return scratchNumeric[col];
    }

    Object scratchVar(int col) {
        return scratchVar[col];
    }

    boolean isStarted() {
        return rowStarted;
    }

    // ── Private helpers ──

    private int addLeaf(String name) {
        int colIdx = schema.appendLeaf(name, parentStack[parentDepth]);
        ensureScratchCapacity(colIdx + 1);
        if (columnsSet.getAndSet(colIdx)) {
            throw new IllegalArgumentException("Duplicate field [" + name + "]");
        }
        return colIdx;
    }

    private void ensureScratchCapacity(int size) {
        if (size <= scratchType.length) {
            return;
        }
        int cap = scratchType.length;
        while (cap < size) {
            cap <<= 1;
        }
        scratchType = Arrays.copyOf(scratchType, cap);
        scratchNumeric = Arrays.copyOf(scratchNumeric, cap);
        scratchVar = Arrays.copyOf(scratchVar, cap);
        columnsSet = FixedBitSet.ensureCapacity(columnsSet, cap);
    }

    private void ensureParentStackCapacity() {
        if (parentDepth < parentStack.length) {
            return;
        }
        parentStack = Arrays.copyOf(parentStack, parentStack.length * 2);
    }
}
