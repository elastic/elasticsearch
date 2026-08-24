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
import org.elasticsearch.columnar.substrate.MonotonicWriter;

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
 * against {@link #dictionary()}; its {@link #values()} stream holds nothing, since a value is either named
 * by a term or held in {@link #exceptions()}.
 *
 * <p>An ordinal equal to {@link #dictionarySize()} is the escape: the value is not in the dictionary and
 * its bytes are in the exceptions stream instead. Which one is found by counting escapes, which
 * {@link #escapeRanks()} makes bounded work by recording how many came before every block of values.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    long valueBytes,
    StringColumnLayout layout,
    ValueStream.Metadata values,
    ValueStream.Metadata dictionary,
    NumericColumnMetadata ordinals,
    ValueStream.Metadata exceptions,
    MonotonicWriter.Table escapeRanks,
    int dictionarySize,
    Summary summary
) implements ColumnMetadata {

    /**
     * What a column records of the terms it holds most, so a merge can work out a vocabulary from its
     * inputs instead of reading their values again. The counts are the survey's, and so are lower bounds.
     *
     * <p>A dictionary column's summary terms are its dictionary; only the counts are written beside it.
     *
     * @param terms        the summarised terms in term order, or null when they are the dictionary
     * @param countsOffset where the counts, one vlong per term, begin
     * @param countsLength how many bytes they occupy
     * @param numValues    the values the survey saw, which the counts are a share of
     */
    public record Summary(ValueStream.Metadata terms, long countsOffset, long countsLength, long numValues) {}

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
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            values.valueBytes(),
            StringColumnLayout.PLAIN,
            values,
            null,
            null,
            null,
            MonotonicWriter.Table.NONE,
            0,
            null
        );
    }

    /**
     * A column that names its values with ordinals into {@code dictionary}. Values the dictionary does not
     * hold escape into {@code exceptions}, found through {@code escapeRanks}.
     */
    public static StringColumnMetadata dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        ValueStream.Metadata exceptions,
        MonotonicWriter.Table escapeRanks,
        int dictionarySize
    ) {
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            valueBytes,
            StringColumnLayout.DICTIONARY,
            ValueStream.Metadata.empty(),
            dictionary,
            ordinals,
            exceptions,
            escapeRanks,
            dictionarySize,
            null
        );
    }

    /** The same column, with what it surveyed recorded beside it. */
    public StringColumnMetadata withSummary(Summary summary) {
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            valueBytes,
            layout,
            values,
            dictionary,
            ordinals,
            exceptions,
            escapeRanks,
            dictionarySize,
            summary
        );
    }

    /** Whether this column recorded what it surveyed. */
    public boolean hasSummary() {
        return summary != null;
    }

    /** Whether any value escaped the dictionary. */
    public boolean hasEscapes() {
        return exceptions != null && exceptions.numValues() > 0;
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
        out.writeVLong(valueBytes);
        out.writeByte(layout.id());
        switch (layout) {
            case PLAIN -> values.writeTo(out);
            case DICTIONARY -> {
                out.writeVInt(dictionarySize);
                dictionary.writeTo(out);
                ordinals.writeTo(out);
                exceptions.writeTo(out);
                if (exceptions.numValues() > 0) {
                    out.writeVLong(escapeRanks.dataOffset());
                    out.writeVLong(escapeRanks.dataLength());
                    out.writeVInt(escapeRanks.meta().length);
                    out.writeBytes(escapeRanks.meta(), 0, escapeRanks.meta().length);
                }
            }
        }
        out.writeByte((byte) (summary == null ? 0 : 1));
        if (summary != null) {
            // A dictionary column's summary terms are its dictionary, so only the counts are written.
            out.writeByte((byte) (summary.terms() == null ? 0 : 1));
            if (summary.terms() != null) {
                summary.terms().writeTo(out);
            }
            out.writeVLong(summary.countsOffset());
            out.writeVLong(summary.countsLength());
            out.writeVLong(summary.numValues());
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
        long valueBytes = in.readVLong();
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        final StringColumnMetadata column = switch (layout) {
            case PLAIN -> plain(iterator, numDocsWithField, numValues, ValueStream.Metadata.readFrom(in));
            case DICTIONARY -> {
                final int dictionarySize = in.readVInt();
                final ValueStream.Metadata dictionary = ValueStream.Metadata.readFrom(in);
                final NumericColumnMetadata ordinals = NumericColumnMetadata.readFrom(in, maxDoc, formatVersion);
                final ValueStream.Metadata exceptions = ValueStream.Metadata.readFrom(in);
                MonotonicWriter.Table escapeRanks = MonotonicWriter.Table.NONE;
                if (exceptions.numValues() > 0) {
                    final long dataOffset = in.readVLong();
                    final long dataLength = in.readVLong();
                    final byte[] meta = new byte[in.readVInt()];
                    in.readBytes(meta, 0, meta.length);
                    escapeRanks = new MonotonicWriter.Table(dataOffset, dataLength, meta);
                }
                yield dictionary(
                    iterator,
                    numDocsWithField,
                    numValues,
                    valueBytes,
                    dictionary,
                    ordinals,
                    exceptions,
                    escapeRanks,
                    dictionarySize
                );
            }
        };
        if (in.readByte() == 0) {
            return column;
        }
        final ValueStream.Metadata summaryTerms = in.readByte() == 0 ? null : ValueStream.Metadata.readFrom(in);
        return column.withSummary(new Summary(summaryTerms, in.readVLong(), in.readVLong(), in.readVLong()));
    }

}
