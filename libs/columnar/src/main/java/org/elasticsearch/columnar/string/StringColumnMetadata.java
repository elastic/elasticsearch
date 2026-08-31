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
 * <p>A column takes one of two layouts, and each says only what its own layout has. {@link Plain} reads its
 * values straight out of {@link Plain#values()}. {@link Dictionary} instead reads an ordinal from
 * {@link Dictionary#ordinals()} and resolves it against {@link Dictionary#dictionary()}; it stores no values
 * of its own, since a value is either named by a term or held in {@link Dictionary#escapes()}.
 *
 * <p>What both layouts have is here: how many documents and values the column holds, what its values would
 * occupy stored plainly, and what it recorded of the terms it holds most.
 */
public sealed interface StringColumnMetadata extends ColumnMetadata permits StringColumnMetadata.Plain, StringColumnMetadata.Dictionary {

    /** The documents that have a value, and where each one's values begin. */
    ColumnIteratorMetadata iterator();

    /** How many documents have a value. */
    int numDocsWithField();

    /** How many values the column holds across all documents. */
    long numValues();

    /** What the column's values would occupy stored plainly, which a decision about its layout is weighed against. */
    long valueBytes();

    /** What the column recorded of the terms it holds most, or null when it recorded nothing. */
    Summary summary();

    /** Which layout the column takes, as written on disk. */
    StringColumnLayout layout();

    /** The same column, with what it surveyed recorded beside it. */
    StringColumnMetadata withSummary(Summary summary);

    /** Writes what this layout has, between the fields both layouts share. */
    void writeBody(DataOutput out) throws IOException;

    /** Whether this column recorded what it surveyed. */
    default boolean hasSummary() {
        return summary() != null;
    }

    /** True when at least one document has more than one value. */
    default boolean multiValued() {
        return numValues() > numDocsWithField();
    }

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
    record Summary(ValueStream.Metadata terms, long countsOffset, long countsLength, long numValues) {}

    /** A column that stores its values as they were written. */
    record Plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata values,
        Summary summary
    ) implements StringColumnMetadata {

        @Override
        public StringColumnLayout layout() {
            return StringColumnLayout.PLAIN;
        }

        @Override
        public Plain withSummary(Summary summary) {
            return new Plain(iterator, numDocsWithField, numValues, valueBytes, values, summary);
        }

        @Override
        public void writeBody(DataOutput out) throws IOException {
            values.writeTo(out);
        }
    }

    /**
     * A column that names its values with ordinals into {@link #dictionary()}. A value the dictionary does
     * not hold escapes into {@link #escapes()}, found by counting the escapes before it, which
     * {@link #escapeRanks()} makes bounded work by recording how many came before every block of values.
     *
     * <p>An ordinal equal to {@link #dictionarySize()} is the escape marker.
     */
    record Dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        ValueStream.Metadata escapes,
        MonotonicWriter.Table escapeRanks,
        int dictionarySize,
        Summary summary
    ) implements StringColumnMetadata {

        @Override
        public StringColumnLayout layout() {
            return StringColumnLayout.DICTIONARY;
        }

        /** Whether any value escaped the dictionary. */
        public boolean hasEscapes() {
            return escapes != null && escapes.numValues() > 0;
        }

        @Override
        public Dictionary withSummary(Summary summary) {
            return new Dictionary(
                iterator,
                numDocsWithField,
                numValues,
                valueBytes,
                dictionary,
                ordinals,
                escapes,
                escapeRanks,
                dictionarySize,
                summary
            );
        }

        @Override
        public void writeBody(DataOutput out) throws IOException {
            out.writeVInt(dictionarySize);
            dictionary.writeTo(out);
            ordinals.writeTo(out);
            escapes.writeTo(out);
            if (escapes.numValues() > 0) {
                out.writeVLong(escapeRanks.dataOffset());
                out.writeVLong(escapeRanks.dataLength());
                out.writeVInt(escapeRanks.meta().length);
                out.writeBytes(escapeRanks.meta(), 0, escapeRanks.meta().length);
            }
        }
    }

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return plain(iterator, 0, 0, ValueStream.Metadata.empty());
    }

    /** A column that stores its values as they were written. */
    static Plain plain(ColumnIteratorMetadata iterator, int numDocsWithField, long numValues, ValueStream.Metadata values) {
        return new Plain(iterator, numDocsWithField, numValues, values.valueBytes(), values, null);
    }

    /** A column that names its values with ordinals into {@code dictionary}. */
    static Dictionary dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        ValueStream.Metadata escapes,
        MonotonicWriter.Table escapeRanks,
        int dictionarySize
    ) {
        return new Dictionary(
            iterator,
            numDocsWithField,
            numValues,
            valueBytes,
            dictionary,
            ordinals,
            escapes,
            escapeRanks,
            dictionarySize,
            null
        );
    }

    @Override
    default void writeTo(DataOutput out) throws IOException {
        iterator().writeTo(out);
        out.writeVInt(numDocsWithField());
        if (numDocsWithField() == 0) {
            return;
        }
        out.writeVLong(numValues());
        out.writeVLong(valueBytes());
        out.writeByte(layout().id());
        writeBody(out);
        final Summary summary = summary();
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
    static StringColumnMetadata readFrom(DataInput in, int maxDoc, final FormatVersion formatVersion) throws IOException {
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
                final ValueStream.Metadata escapes = ValueStream.Metadata.readFrom(in);
                MonotonicWriter.Table escapeRanks = MonotonicWriter.Table.NONE;
                if (escapes.numValues() > 0) {
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
                    escapes,
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
