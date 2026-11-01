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
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;

/**
 * Mutable single-row staging buffer for ESCF column-major encoding — the write-side dual of
 * {@link org.elasticsearch.sourcebatch.SourceRow}. Does not implement {@code SourceRow} because
 * there are no finished columns, the buffer is partial and mutable, and it grows the schema as
 * fields are encountered.
 *
 * <p>Usage: call {@link #beginRow()} (or {@link EscfBatchBuilder#beginRow()}), populate fields
 * via {@link #startObject}/{@link #endObject} and the leaf writers, call {@link #finishRow()},
 * then call {@link EscfBatchBuilder#commit} to drain into column builders.
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
     * Whether {@link #finishRow()} has been called but {@link EscfBatchBuilder#commit} has not.
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

    /** Resets the buffer for a new row. Must be called before any field writes. */
    public void beginRow() {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratchType, 0, Math.min(columnCountBefore, scratchType.length), (byte) 0);
        Arrays.fill(scratchVar, 0, Math.min(columnCountBefore, scratchVar.length), null);
        columnsSet.clear();
        parentDepth = 0;
        rowStarted = false;
    }

    /**
     * Marks the row as fully staged and ready to commit. Must be called after all field writes
     * succeed; if parsing throws before this point, {@link EscfBatchBuilder#commit} will reject
     * the half-populated row rather than draining it silently.
     */
    public void finishRow() {
        rowStarted = true;
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

    /** Encodes an empty object as a zero-byte {@code KEY_VALUE} leaf, keeping it distinct from absent. Returns the leaf column index. */
    public int emptyObject(String name) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.KEY_VALUE;
        scratchVar[colIdx] = BytesRef.EMPTY_BYTES;
        return colIdx;
    }

    /** Stages a long; uses {@code INT} encoding if the value fits in int range. Returns the leaf column index. */
    public int longField(String name, long value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) ? SourceValueType.INT : SourceValueType.LONG;
        scratchNumeric[colIdx] = value;
        return colIdx;
    }

    /** Stages a double; uses {@code FLOAT} encoding if it round-trips exactly through float. Returns the leaf column index. */
    public int doubleField(String name, double value) {
        int colIdx = addLeaf(name);
        float fval = (float) value;
        scratchType[colIdx] = ((double) fval == value) ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
        scratchNumeric[colIdx] = Double.doubleToRawLongBits(value);
        return colIdx;
    }

    /** Stages a UTF-8 string value. Returns the leaf column index. */
    public int stringField(String name, XContentString.UTF8Bytes value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.STRING;
        scratchVar[colIdx] = value;
        return colIdx;
    }

    /** Stages a UTF-8 string value from a raw byte slice. Returns the leaf column index. */
    public int stringField(String name, byte[] bytes, int offset, int length) {
        return stringField(name, new XContentString.UTF8Bytes(bytes, offset, length));
    }

    /** Stages a boolean value. Returns the leaf column index. */
    public int booleanField(String name, boolean value) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = value ? SourceValueType.TRUE : SourceValueType.FALSE;
        return colIdx;
    }

    /** Stages an explicit null. Returns the leaf column index. */
    public int nullField(String name) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = SourceValueType.NULL;
        return colIdx;
    }

    /**
     * Stages a packed inline array ({@link SourceValueType#FIXED_ARRAY} or
     * {@link SourceValueType#UNION_ARRAY}). Returns the leaf column index.
     */
    public int arrayField(String name, byte arrayType, byte[] packed) {
        int colIdx = addLeaf(name);
        scratchType[colIdx] = arrayType;
        scratchVar[colIdx] = packed;
        return colIdx;
    }

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
        int cap = ArrayUtil.oversize(size, Byte.BYTES);
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
