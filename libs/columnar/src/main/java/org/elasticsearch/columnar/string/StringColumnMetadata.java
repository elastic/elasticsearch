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
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;

import java.io.IOException;

/**
 * Describes a string column. Values live in one value-address-indexed, block-encoded store in the order they were
 * written (never reordered), addressed by a compact {@code DirectMonotonic} table of per-block byte offsets. The
 * offset table is per block rather than per value so its size is a fraction of the column's — the position of a
 * value inside its block comes from decoding the block, which a read has to do anyway.
 *
 * <p>{@link #layout()} says how a block is encoded. Only {@link StringColumnLayout#PLAIN} exists today; the
 * recorded layout id is the extension point a later ordinal layout arrives on, so which trailing fields are
 * meaningful can vary by layout.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    int blockSize,
    byte blockBytesCodecId,
    StringColumnLayout layout,
    long valuesOffset,
    long blockOffsetsDataOffset,
    long blockOffsetsDataLength,
    byte[] blockOffsetsMeta
) implements ColumnMetadata {
    private static final byte[] NONE = new byte[0];

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator, byte blockBytesCodecId) {
        return new StringColumnMetadata(iterator, 0, 0, 0, blockBytesCodecId, StringColumnLayout.PLAIN, 0, 0, 0, NONE);
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
        out.writeVInt(blockSize);
        out.writeByte(blockBytesCodecId);
        out.writeByte(layout.id());
        out.writeVLong(valuesOffset);
        out.writeVLong(blockOffsetsDataOffset);
        out.writeVLong(blockOffsetsDataLength);
        out.writeVInt(blockOffsetsMeta.length);
        out.writeBytes(blockOffsetsMeta, 0, blockOffsetsMeta.length);
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
            return empty(iterator, BlockBytesCodec.IDENTITY_ID);
        }
        long numValues = in.readVLong();
        int blockSize = in.readVInt();
        byte blockBytesCodecId = in.readByte();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        long valuesOffset = in.readVLong();
        long blockOffsetsDataOffset = in.readVLong();
        long blockOffsetsDataLength = in.readVLong();
        byte[] blockOffsetsMeta = readBytes(in);
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            blockSize,
            blockBytesCodecId,
            layout,
            valuesOffset,
            blockOffsetsDataOffset,
            blockOffsetsDataLength,
            blockOffsetsMeta
        );
    }

    private static byte[] readBytes(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return bytes;
    }

    long numBlocks() {
        return numValues == 0 ? 0 : (numValues + blockSize - 1) / blockSize;
    }
}
