/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.ColumnMetadata;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;

import java.io.IOException;

/**
 * Describes a string column. Under {@link StringColumnLayout#PLAIN} the values are one byte blob in the order
 * they were written (never reordered), addressed by a compact {@code DirectMonotonic} table holding the byte
 * offset of every value. There is no block: a value's bytes are exactly the span between its offset and the
 * next, so a read is two offset lookups and one range read, and the length needs no prefix on disk.
 *
 * <p>{@link #layout()} is recorded so a later layout — one carrying ordinals, say — arrives as a new id and can
 * bring its own trailing fields.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    StringColumnLayout layout,
    long valuesOffset,
    long valueOffsetsDataOffset,
    long valueOffsetsDataLength,
    byte[] valueOffsetsMeta
) implements ColumnMetadata {
    private static final byte[] NONE = new byte[0];

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return new StringColumnMetadata(iterator, 0, 0, StringColumnLayout.PLAIN, 0, 0, 0, NONE);
    }

    /** True when at least one document has more than one value. */
    public boolean multiValued() {
        return numValues > numDocsWithField;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        iterator.writeTo(out);
        out.writeVInt(numDocsWithField);
        if (numDocsWithField == 0) {
            return;
        }
        out.writeVLong(numValues);
        out.writeByte(layout.id());
        out.writeVLong(valuesOffset);
        out.writeVLong(valueOffsetsDataOffset);
        out.writeVLong(valueOffsetsDataLength);
        out.writeVInt(valueOffsetsMeta.length);
        out.writeBytes(valueOffsetsMeta, 0, valueOffsetsMeta.length);
    }

    /**
     * Reads metadata written by {@link #writeTo}.
     *
     * <p>{@code formatVersion} is the on-disk version returned by
     * {@link org.elasticsearch.columnar.substrate.ColumnarCodecUtil#checkHeader}. Fields added in a later
     * layout version are gated on it:
     * <pre>{@code
     * if (formatVersion.onOrAfter(FormatVersion.V1_EXTRA_FLAGS)) {
     *     flags = in.readVInt();
     * }
     * }</pre>
     */
    public static StringColumnMetadata readFrom(DataInput in, int maxDoc, final FormatVersion formatVersion) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorMetadata.readFrom(in, maxDoc, formatVersion);
        int numDocsWithField = in.readVInt();
        if (numDocsWithField == 0) {
            return empty(iterator);
        }
        long numValues = in.readVLong();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        long valuesOffset = in.readVLong();
        long valueOffsetsDataOffset = in.readVLong();
        long valueOffsetsDataLength = in.readVLong();
        byte[] valueOffsetsMeta = readBytes(in);
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            layout,
            valuesOffset,
            valueOffsetsDataOffset,
            valueOffsetsDataLength,
            valueOffsetsMeta
        );
    }

    private static byte[] readBytes(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return bytes;
    }
}
