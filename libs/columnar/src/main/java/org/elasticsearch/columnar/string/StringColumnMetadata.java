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
    StringColumnLayout layout,
    ValueStream.Metadata values
) implements ColumnMetadata {
    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return new StringColumnMetadata(iterator, 0, 0, StringColumnLayout.PLAIN, ValueStream.Metadata.empty());
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
        values.writeTo(out);
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
        return new StringColumnMetadata(iterator, numDocsWithField, numValues, layout, ValueStream.Metadata.readFrom(in));
    }

}
