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
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;

import java.io.IOException;

/**
 * Describes a string column. Values live in one value-address-indexed, block-encoded store in the order they were
 * written (never reordered), addressed by a compact {@code DirectMonotonic} table of per-block byte offsets. The
 * offset table is per block rather than per value so its size is a fraction of the column's — the position of a
 * value inside its block comes from decoding the block, which a read has to do anyway.
 *
 * <p>{@link #layout()} says how a block is encoded, and which trailing fields are meaningful.
 * {@link StringColumnLayout#PLAIN} reads its values straight out of {@link #values()}.
 * {@link StringColumnLayout#DICTIONARY} instead reads an ordinal from {@link #ordinals()} and resolves it
 * against {@link #dictionary()}; its {@link #values()} stream holds nothing, since every value is named by
 * a term.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    StringColumnLayout layout,
    ValueStream.Metadata values,
    ValueStream.Metadata dictionary,
    NumericColumnMetadata ordinals,
    int dictionarySize
) implements ColumnMetadata {
    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return plain(iterator, 0, 0, ValueStream.Metadata.empty());
    }

    /** A column that stores its values as they were written. */
    public static StringColumnMetadata plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        ValueStream.Metadata values
    ) {
        return new StringColumnMetadata(iterator, numDocsWithField, numValues, StringColumnLayout.PLAIN, values, null, null, 0);
    }

    /** A column that names every value with an ordinal into {@code dictionary}. */
    public static StringColumnMetadata dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        int dictionarySize
    ) {
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            StringColumnLayout.DICTIONARY,
            ValueStream.Metadata.empty(),
            dictionary,
            ordinals,
            dictionarySize
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
        out.writeByte(layout.id());
        switch (layout) {
            case PLAIN -> values.writeTo(out);
            case DICTIONARY -> {
                out.writeVInt(dictionarySize);
                dictionary.writeTo(out);
                ordinals.writeTo(out);
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
            return empty(iterator);
        }
        long numValues = in.readVLong();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        return switch (layout) {
            case PLAIN -> plain(iterator, numDocsWithField, numValues, ValueStream.Metadata.readFrom(in));
            case DICTIONARY -> {
                final int dictionarySize = in.readVInt();
                final ValueStream.Metadata dictionary = ValueStream.Metadata.readFrom(in);
                yield dictionary(
                    iterator,
                    numDocsWithField,
                    numValues,
                    dictionary,
                    NumericColumnMetadata.readFrom(in, maxDoc, formatVersion),
                    dictionarySize
                );
            }
        };
    }

}
