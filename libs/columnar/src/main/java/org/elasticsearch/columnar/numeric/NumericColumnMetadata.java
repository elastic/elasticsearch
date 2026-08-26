/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.ColumnMetadata;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;

import java.io.IOException;

/**
 * Describes a numeric column — single- or multi-valued, in one format. Values live in one
 * value-address-indexed, block-encoded store in the order they were written (never reordered). Two compact
 * {@code DirectMonotonic} tables address it:
 *
 * <ul>
 *   <li><b>block offsets</b> — the byte position of each value block; always present.</li>
 *   <li><b>value addresses</b> — the first value address of each document, present only when the
 *       column is multi-valued ({@code numValues > numDocsWithField}). When every document has one
 *       value the table is dropped and a document's value address is its rank.</li>
 * </ul>
 *
 * Each table stores its data in the data file (read off-heap from the mapped input) and its small
 * monotonic-block metadata here.
 */
public record NumericColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    int blockSize,
    byte blockBytesCodecId,
    byte terminalId,
    byte[] transformIds,
    long valuesOffset,
    long blockOffsetsDataOffset,
    long blockOffsetsDataLength,
    byte[] blockOffsetsMeta,
    long valueAddressesDataOffset,
    long valueAddressesDataLength,
    byte[] valueAddressesMeta,
    Skipper skipper
) implements ColumnMetadata {
    private static final byte[] NONE = new byte[0];

    /** Descriptor of the default pipeline, stored on empty columns that serialize no block payload. */
    private static final byte DEFAULT_TERMINAL_ID = ForTerminal.ID;
    private static final byte[] DEFAULT_TRANSFORM_IDS = { DeltaTransform.ID, OffsetTransform.ID, GcdTransform.ID };

    /**
     * The doc-values skip index for a range-indexed column: a multi-level structure of per-interval
     * value bounds and doc-id ranges written into the skip-index file, plus the column-wide summary. Present
     * only when the field carries a range skip index.
     *
     * @param dataOffset    start of the skip region in the skip-index file
     * @param dataLength    length of the skip region
     * @param minValue      smallest value in the column
     * @param maxValue      largest value in the column
     * @param docCount      documents with a value
     * @param maxDocId      largest doc id with a value
     * @param maxValueCount most values held by any single document
     * @param codecId       {@link SkipIndexCodec} id that wrote the region, selecting the reader
     */
    public record Skipper(
        long dataOffset,
        long dataLength,
        long minValue,
        long maxValue,
        int docCount,
        int maxDocId,
        int maxValueCount,
        byte codecId
    ) {
        void writeTo(DataOutput out) throws IOException {
            out.writeVLong(dataOffset);
            out.writeVLong(dataLength);
            out.writeLong(minValue);
            out.writeLong(maxValue);
            out.writeVInt(docCount);
            out.writeVInt(maxDocId);
            out.writeVInt(maxValueCount);
            out.writeByte(codecId);
        }

        static Skipper readFrom(DataInput in) throws IOException {
            return new Skipper(
                in.readVLong(),
                in.readVLong(),
                in.readLong(),
                in.readLong(),
                in.readVInt(),
                in.readVInt(),
                in.readVInt(),
                in.readByte()
            );
        }
    }

    static NumericColumnMetadata empty(ColumnIteratorMetadata iterator, byte blockBytesCodecId) {
        return new NumericColumnMetadata(
            iterator,
            0,
            0,
            0,
            blockBytesCodecId,
            DEFAULT_TERMINAL_ID,
            DEFAULT_TRANSFORM_IDS,
            0,
            0,
            0,
            NONE,
            0,
            0,
            NONE,
            null
        );
    }

    /** A copy of this metadata carrying {@code skipper}. */
    public NumericColumnMetadata withSkipper(Skipper skipper) {
        return new NumericColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            blockSize,
            blockBytesCodecId,
            terminalId,
            transformIds,
            valuesOffset,
            blockOffsetsDataOffset,
            blockOffsetsDataLength,
            blockOffsetsMeta,
            valueAddressesDataOffset,
            valueAddressesDataLength,
            valueAddressesMeta,
            skipper
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
        out.writeVLong(numValues);
        out.writeVInt(blockSize);
        out.writeByte(blockBytesCodecId);
        out.writeByte(terminalId);
        out.writeVInt(transformIds.length);
        out.writeBytes(transformIds, 0, transformIds.length);
        out.writeVLong(valuesOffset);
        writeTable(out, blockOffsetsDataOffset, blockOffsetsDataLength, blockOffsetsMeta);
        if (multiValued()) {
            writeTable(out, valueAddressesDataOffset, valueAddressesDataLength, valueAddressesMeta);
        }
        out.writeByte((byte) (skipper != null ? 1 : 0));
        if (skipper != null) {
            skipper.writeTo(out);
        }
    }

    /**
     * Reads a column metadata record previously written by {@link #writeTo}.
     *
     * <p>{@code formatVersion} is the on-disk version returned by
     * {@link org.elasticsearch.columnar.substrate.ColumnarCodecUtil#checkHeader}; it has already
     * been validated to be in {@code [BASELINE, CURRENT]}. When a future version adds a field
     * to this layout, gate the read on a {@code VERSION_*} constant from
     * {@link org.elasticsearch.columnar.FormatVersion}:
     * <pre>{@code
     * int flags = 0;
     * if (formatVersion.onOrAfter(FormatVersion.V1_EXTRA_FLAGS)) {
     *     flags = in.readVInt();
     * }
     * }</pre>
     * Without this branch, an old reader decoding a next-version segment would consume the flag
     * bytes as part of the next field and corrupt every subsequent offset.
     */
    public static NumericColumnMetadata readFrom(DataInput in, int maxDoc, final FormatVersion formatVersion) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorMetadata.readFrom(in, maxDoc, formatVersion);
        int numDocsWithField = in.readVInt();
        if (numDocsWithField == 0) {
            return empty(iterator, BlockBytesCodec.IDENTITY_ID);
        }
        long numValues = in.readVLong();
        int blockSize = in.readVInt();
        byte blockBytesCodecId = in.readByte();
        byte terminalId = in.readByte();
        byte[] transformIds = readBytes(in);
        long valuesOffset = in.readVLong();
        long blockOffsetsDataOffset = in.readVLong();
        long blockOffsetsDataLength = in.readVLong();
        byte[] blockOffsetsMeta = readBytes(in);
        long valueAddressesDataOffset = 0;
        long valueAddressesDataLength = 0;
        byte[] valueAddressesMeta = NONE;
        if (numValues > numDocsWithField) {
            valueAddressesDataOffset = in.readVLong();
            valueAddressesDataLength = in.readVLong();
            valueAddressesMeta = readBytes(in);
        }
        Skipper skipper = in.readByte() == 1 ? Skipper.readFrom(in) : null;
        return new NumericColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            blockSize,
            blockBytesCodecId,
            terminalId,
            transformIds,
            valuesOffset,
            blockOffsetsDataOffset,
            blockOffsetsDataLength,
            blockOffsetsMeta,
            valueAddressesDataOffset,
            valueAddressesDataLength,
            valueAddressesMeta,
            skipper
        );
    }

    private static void writeTable(DataOutput out, long dataOffset, long dataLength, byte[] meta) throws IOException {
        out.writeVLong(dataOffset);
        out.writeVLong(dataLength);
        out.writeVInt(meta.length);
        out.writeBytes(meta, 0, meta.length);
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
