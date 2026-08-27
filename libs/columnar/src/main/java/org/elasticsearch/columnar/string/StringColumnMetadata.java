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
 * Describes a string column — single- or multi-valued. Slots live in one value-address-indexed,
 * block-encoded store in the order they were written (never reordered), addressed by a compact
 * {@code DirectMonotonic} table of per-block byte offsets. The offset table is per block rather than per
 * value so its size is a fraction of the column's — the position of a value inside its block comes from
 * decoding the block, which a read has to do anyway.
 *
 * <p>Two further {@code DirectMonotonic} tables address the column, each written only when it has anything
 * to say:
 *
 * <ul>
 *   <li><b>value addresses</b> — the first value address of each document, present only when the column is
 *       multi-valued ({@code numValues > numDocsWithField}). When every document holds one slot the table is
 *       dropped and a document's value address is its rank.</li>
 *   <li><b>null slots</b> — the value addresses that hold a null slot, in increasing order, present only when
 *       {@code numNullSlots > 0}. A null occupies an address like any other slot (its bytes are stored as a
 *       zero-length value), so one address space covers the whole column and {@code valueCount(rank)} is the
 *       slot count including nulls.</li>
 * </ul>
 *
 * Each table stores its data in the data file (read off-heap from the mapped input) and its small
 * monotonic-block metadata here.
 *
 * <p>{@link #layout()} says how a block is encoded. Only {@link StringColumnLayout#PLAIN} exists today; the
 * recorded layout id is the extension point a later ordinal layout arrives on, so which trailing fields are
 * meaningful can vary by layout. {@link #framing()} is separate and orthogonal: it is not how the column
 * stores anything, but which of the mapper's framings {@code binaryValue()} re-encodes into.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    long numNullSlots,
    StringColumnLayout layout,
    StringBinaryPayload.Framing framing,
    ValueStream.Metadata values,
    long valueAddressesDataOffset,
    long valueAddressesDataLength,
    byte[] valueAddressesMeta,
    long nullSlotsDataOffset,
    long nullSlotsDataLength,
    byte[] nullSlotsMeta
) implements ColumnMetadata {

    private static final byte[] NONE = new byte[0];

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return new StringColumnMetadata(
            iterator,
            0,
            0,
            0,
            StringColumnLayout.PLAIN,
            StringBinaryPayload.Framing.SEPARATE_COUNT,
            ValueStream.Metadata.empty(),
            0,
            0,
            NONE,
            0,
            0,
            NONE
        );
    }

    /** True when at least one document has more than one slot. */
    public boolean multiValued() {
        return numValues > numDocsWithField;
    }

    /** True when at least one slot in the column is null. */
    public boolean hasNullSlots() {
        return numNullSlots > 0;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        iterator.writeTo(out);
        out.writeVInt(numDocsWithField);
        if (numDocsWithField == 0) {
            return;
        }
        out.writeVLong(numValues);
        out.writeVLong(numNullSlots);
        out.writeByte(layout.id());
        out.writeByte(framing.id());
        values.writeTo(out);
        if (multiValued()) {
            writeTable(out, valueAddressesDataOffset, valueAddressesDataLength, valueAddressesMeta);
        }
        if (hasNullSlots()) {
            writeTable(out, nullSlotsDataOffset, nullSlotsDataLength, nullSlotsMeta);
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
     *
     * <p>The two trailing tables need no such gate: each is written only when a field already on the wire
     * ahead of it says so, so a reader knows whether to expect one before it gets there.
     */
    public static StringColumnMetadata readFrom(DataInput in, int maxDoc, final FormatVersion formatVersion) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorMetadata.readFrom(in, maxDoc, formatVersion);
        int numDocsWithField = in.readVInt();
        if (numDocsWithField == 0) {
            return empty(iterator);
        }
        long numValues = in.readVLong();
        long numNullSlots = in.readVLong();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        StringBinaryPayload.Framing framing = StringBinaryPayload.Framing.forId(in.readByte());
        ValueStream.Metadata values = ValueStream.Metadata.readFrom(in);
        long valueAddressesDataOffset = 0;
        long valueAddressesDataLength = 0;
        byte[] valueAddressesMeta = NONE;
        if (numValues > numDocsWithField) {
            valueAddressesDataOffset = in.readVLong();
            valueAddressesDataLength = in.readVLong();
            valueAddressesMeta = readBytes(in);
        }
        long nullSlotsDataOffset = 0;
        long nullSlotsDataLength = 0;
        byte[] nullSlotsMeta = NONE;
        if (numNullSlots > 0) {
            nullSlotsDataOffset = in.readVLong();
            nullSlotsDataLength = in.readVLong();
            nullSlotsMeta = readBytes(in);
        }
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            numNullSlots,
            layout,
            framing,
            values,
            valueAddressesDataOffset,
            valueAddressesDataLength,
            valueAddressesMeta,
            nullSlotsDataOffset,
            nullSlotsDataLength,
            nullSlotsMeta
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

}
