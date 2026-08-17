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
 * written (never reordered), addressed by a compact {@code DirectMonotonic} table of per-block byte offsets.
 *
 * <p>{@link #layout()} says how a block is encoded, and which of the trailing fields are meaningful:
 * <ul>
 *   <li>{@link StringColumnLayout#PLAIN} — {@link #maxBlockValueBytes()} sizes the reader's block scratch.</li>
 *   <li>{@link StringColumnLayout#DICTIONARY} — {@link #dictionary()} holds the segment's terms and
 *       {@link #terminalId()} / {@link #transformIds()} describe the pipeline the ordinal stream was encoded
 *       with, so the reader rebuilds it exactly.</li>
 * </ul>
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    int numValues,
    int blockSize,
    byte blockBytesCodecId,
    StringColumnLayout layout,
    long valuesOffset,
    long blockOffsetsDataOffset,
    long blockOffsetsDataLength,
    byte[] blockOffsetsMeta,
    int maxBlockValueBytes,
    byte terminalId,
    byte[] transformIds,
    StringDictionary dictionary
) implements ColumnMetadata {
    private static final byte[] NONE = new byte[0];

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator, byte blockBytesCodecId) {
        return new StringColumnMetadata(
            iterator,
            0,
            0,
            0,
            blockBytesCodecId,
            StringColumnLayout.PLAIN,
            0,
            0,
            0,
            NONE,
            0,
            (byte) 0,
            NONE,
            null
        );
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
        out.writeVInt(numValues);
        out.writeVInt(blockSize);
        out.writeByte(blockBytesCodecId);
        out.writeByte(layout.id());
        out.writeVLong(valuesOffset);
        out.writeVLong(blockOffsetsDataOffset);
        out.writeVLong(blockOffsetsDataLength);
        out.writeVInt(blockOffsetsMeta.length);
        out.writeBytes(blockOffsetsMeta, 0, blockOffsetsMeta.length);
        switch (layout) {
            case PLAIN -> out.writeVInt(maxBlockValueBytes);
            case DICTIONARY -> {
                out.writeByte(terminalId);
                out.writeVInt(transformIds.length);
                out.writeBytes(transformIds, 0, transformIds.length);
                dictionary.writeTo(out);
            }
        }
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
        int numValues = in.readVInt();
        int blockSize = in.readVInt();
        byte blockBytesCodecId = in.readByte();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        long valuesOffset = in.readVLong();
        long blockOffsetsDataOffset = in.readVLong();
        long blockOffsetsDataLength = in.readVLong();
        byte[] blockOffsetsMeta = readBytes(in);
        int maxBlockValueBytes = 0;
        byte terminalId = 0;
        byte[] transformIds = NONE;
        StringDictionary dictionary = null;
        switch (layout) {
            case PLAIN -> maxBlockValueBytes = in.readVInt();
            case DICTIONARY -> {
                terminalId = in.readByte();
                transformIds = readBytes(in);
                dictionary = StringDictionary.readFrom(in);
            }
        }
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
            blockOffsetsMeta,
            maxBlockValueBytes,
            terminalId,
            transformIds,
            dictionary
        );
    }

    private static byte[] readBytes(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return bytes;
    }

    int numBlocks() {
        return numValues == 0 ? 0 : (int) ((numValues + (long) blockSize - 1) / blockSize);
    }
}
