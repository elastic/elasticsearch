/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.FormatVersion;

import java.io.IOException;

/**
 * Where and how a field's column-iterator structure is stored, held in the meta stream and used to rebuild
 * a {@link ColumnIterator} on read. Three shapes, distinguished by {@link #offset}:
 *
 * <ul>
 *   <li>{@link #OFFSET_EMPTY} — no document has a value; nothing is written to the data file.</li>
 *   <li>{@link #OFFSET_DENSE} — every document has a value; nothing is written to the data file
 *       because the document id is its own value ordinal.</li>
 *   <li>a non-negative offset — a sparse structure written at {@code [offset, offset + length)}.</li>
 * </ul>
 *
 * {@code maxDoc} is not persisted; it is known from the segment and supplied on read.
 */
public record ColumnIteratorMetadata(
    long offset,
    long length,
    short jumpTableEntryCount,
    byte denseRankPower,
    int numDocsWithField,
    int maxDoc
) {
    /** Offset sentinel meaning no document has a value. */
    public static final long OFFSET_EMPTY = -2;
    /** Offset sentinel meaning every document has a value. */
    public static final long OFFSET_DENSE = -1;

    public boolean isEmpty() {
        return offset == OFFSET_EMPTY;
    }

    public boolean isDense() {
        return offset == OFFSET_DENSE;
    }

    public boolean isSparse() {
        return offset >= 0;
    }

    static ColumnIteratorMetadata empty(int maxDoc) {
        return new ColumnIteratorMetadata(OFFSET_EMPTY, 0L, (short) -1, (byte) -1, 0, maxDoc);
    }

    static ColumnIteratorMetadata dense(int maxDoc) {
        return new ColumnIteratorMetadata(OFFSET_DENSE, 0L, (short) -1, (byte) -1, maxDoc, maxDoc);
    }

    /** Writes everything except {@code maxDoc}, which the reader already knows. */
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(length);
        out.writeShort(jumpTableEntryCount);
        out.writeByte(denseRankPower);
        out.writeVInt(numDocsWithField);
    }

    /**
     * Reads a {@link ColumnIteratorMetadata} record previously written by {@link #writeTo}.
     * {@code formatVersion} is the on-disk version from the segment header; future layout changes
     * gate their reads on {@link org.elasticsearch.columnar.FormatVersion#version()} compared
     * against a {@code VERSION_*} constant.
     */
    public static ColumnIteratorMetadata readFrom(DataInput in, int maxDoc, final FormatVersion formatVersion) throws IOException {
        long offset = in.readLong();
        long length = in.readLong();
        short jumpTableEntryCount = in.readShort();
        byte denseRankPower = in.readByte();
        int numDocsWithField = in.readVInt();
        return new ColumnIteratorMetadata(offset, length, jumpTableEntryCount, denseRankPower, numDocsWithField, maxDoc);
    }
}
