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
 * Describes a string column — single- or multi-valued. Slots live in one value-address-indexed,
 * block-encoded store in the order they were written (never reordered), addressed by a compact
 * {@code DirectMonotonic} table of per-block byte offsets. The offset table is per block rather than per
 * value so its size is a fraction of the column's — the position of a value inside its block comes from
 * decoding the block, which a read has to do anyway.
 *
 * <p>A column takes one of two layouts, and each says only what its own layout has. {@link Plain} reads its
 * values straight out of {@link Plain#values()}. {@link Dictionary} instead reads an ordinal from
 * {@link Dictionary#ordinals()} and resolves it against {@link Dictionary#dictionary()}; it stores no values
 * of its own, since a value is either named by a term or held in {@link Dictionary#escapes()}.
 *
 * <p>What both layouts have is here: how many documents and values the column holds, how many of those slots
 * are null, what the values would occupy stored plainly, where each document's slots begin
 * ({@link #valueAddresses()}), whether the values arrive in order, and what the column recorded of the terms
 * it holds most.
 *
 * <p>Where the nulls are is not shared, because the two layouts can afford different answers. {@link Plain}
 * stores a null as a zero-length value and keeps a table of the addresses that hold one. {@link Dictionary}
 * names a null with a reserved ordinal, which costs no table and keeps a null out of every term's ordinal
 * range — so a query answered from the ordinals alone is not also answering for the empty term.
 */
public sealed interface StringColumnMetadata extends ColumnMetadata permits StringColumnMetadata.Plain, StringColumnMetadata.Dictionary {

    /** The documents that have a value, and where each one's values begin. */
    ColumnIteratorMetadata iterator();

    /** How many documents have a value. */
    int numDocsWithField();

    /** How many slots the column holds across all documents, null slots included. */
    long numValues();

    /**
     * How many of the column's slots are null. Recorded whichever layout follows, and for both the same
     * reason: a merge that would otherwise count its inputs reads this off each of them, and the layout is
     * not chosen until after the count is in. Which layout it is decides where the nulls themselves are
     * recorded, not whether the count is.
     */
    long numNullSlots();

    /** What the column's values would occupy stored plainly, which a decision about its layout is weighed against. */
    long valueBytes();

    /**
     * The first value address of each document, and one past the end, present only when the slots and the
     * documents are not in step. When every document holds exactly one slot the table is dropped and a
     * document's value address is its rank.
     *
     * <p>Shared by both layouts, unlike the nulls: finding where a document's slots are is the same question
     * whichever layout names them. The table stores its data in the data file, read off-heap from the mapped
     * input, and its small monotonic-block metadata here.
     */
    MonotonicWriter.Table valueAddresses();

    /**
     * Whether the column's values arrive in non-decreasing term order, as they do under an index sort on
     * this field. A term is then a contiguous run of ranks, which a search can find by bisecting the values
     * instead of comparing every one of them.
     */
    /** What the sorted flag is written as, so the two sides of the wire name it rather than spell 1 and 0. */
    byte SORTED = 1;
    byte NOT_SORTED = 0;

    boolean valuesSorted();

    /** What the column recorded of the terms it holds most, or null when it recorded nothing. */
    Summary summary();

    /** Which layout the column takes, as written on disk. */
    StringColumnLayout layout();

    /** The same column, with what it surveyed recorded beside it. */
    StringColumnMetadata withSummary(Summary summary);

    /** Writes what this layout has, after the fields both layouts share. */
    void writeBody(DataOutput out) throws IOException;

    /** Whether this column recorded what it surveyed. */
    default boolean hasSummary() {
        return summary() != null;
    }

    /** True when at least one document has more than one slot. */
    default boolean multiValued() {
        return numValues() > numDocsWithField();
    }

    /**
     * Whether a document's value address has to be looked up rather than being its rank. That is any column
     * where the slots and the documents are not in step, which a document holding several slots causes and a
     * document holding none — an empty array — causes just as much.
     */
    default boolean hasValueAddresses() {
        return numValues() != numDocsWithField();
    }

    /** True when at least one slot in the column is null. */
    default boolean hasNullSlots() {
        return numNullSlots() > 0;
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

    /**
     * A column that stores its values as they were written.
     *
     * <p>A null is stored as a zero-length value, so it occupies an address like any other slot and
     * {@link #nullSlots()} is the only thing that tells it from an empty string. That table is this layout's
     * alone: the values are bytes, and bytes have no spare value to mean "null" the way an ordinal does.
     *
     * @param nullSlots the value addresses holding a null, ascending; present only when {@code numNullSlots > 0}
     */
    record Plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long numNullSlots,
        long valueBytes,
        MonotonicWriter.Table valueAddresses,
        MonotonicWriter.Table nullSlots,
        ValueStream.Metadata values,
        boolean valuesSorted,
        Summary summary
    ) implements StringColumnMetadata {

        @Override
        public StringColumnLayout layout() {
            return StringColumnLayout.PLAIN;
        }

        @Override
        public Plain withSummary(Summary summary) {
            return new Plain(
                iterator,
                numDocsWithField,
                numValues,
                numNullSlots,
                valueBytes,
                valueAddresses,
                nullSlots,
                values,
                valuesSorted,
                summary
            );
        }

        @Override
        public void writeBody(DataOutput out) throws IOException {
            values.writeTo(out);
            if (hasNullSlots()) {
                writeTable(out, nullSlots);
            }
        }
    }

    /**
     * A column that names its values with ordinals into {@link #dictionary()}. A value the dictionary does
     * not hold escapes into {@link #escapes()}, found by counting the escapes before it, which
     * {@link #escapeRanks()} makes bounded work by recording how many came before every block of values.
     *
     * <p>Two ordinals are reserved. {@link #NULL_ORDINAL} names a null, so the {@link #dictionarySize()} terms
     * take the ordinals from {@link #FIRST_TERM_ORDINAL} up, and {@link #escapeOrdinal()} — the first ordinal
     * past them — marks a value the dictionary does not hold. Naming a null rather than tabling it keeps this
     * layout's hot path free of the
     * question: a null is in no term's ordinal range, so a query resolved against the dictionary excludes
     * nulls without a second lookup and can never confuse one with the empty term. It also keeps nulls out of
     * the escapes, which a column-wide {@code escapeCount == 0} is worth staying on the right side of.
     *
     * <p>Zero rather than another value above the terms, so recognising a null takes no knowledge of how
     * large the dictionary is and adding a term never moves it.
     */
    record Dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long numNullSlots,
        long valueBytes,
        MonotonicWriter.Table valueAddresses,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        ValueStream.Metadata escapes,
        MonotonicWriter.Table escapeRanks,
        int dictionarySize,
        boolean valuesSorted,
        Summary summary
    ) implements StringColumnMetadata {

        @Override
        public StringColumnLayout layout() {
            return StringColumnLayout.DICTIONARY;
        }

        /** The ordinal naming a null slot. Fixed, so it costs a reader nothing to know. */
        public static final int NULL_ORDINAL = 0;

        /** The ordinal of the first term, the reserved null taking the one below it. */
        public static final int FIRST_TERM_ORDINAL = NULL_ORDINAL + 1;

        /** Whether any value escaped the dictionary. */
        public boolean hasEscapes() {
            return escapes != null && escapes.numValues() > 0;
        }

        /** The ordinal marking a value no term names, one past the last term. */
        public int escapeOrdinal() {
            return dictionarySize + FIRST_TERM_ORDINAL;
        }

        @Override
        public Dictionary withSummary(Summary summary) {
            return new Dictionary(
                iterator,
                numDocsWithField,
                numValues,
                numNullSlots,
                valueBytes,
                valueAddresses,
                dictionary,
                ordinals,
                escapes,
                escapeRanks,
                dictionarySize,
                valuesSorted,
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
                writeTable(out, escapeRanks);
            }
        }
    }

    static StringColumnMetadata empty(ColumnIteratorMetadata iterator) {
        return plain(iterator, 0, 0, 0, MonotonicWriter.Table.NONE, MonotonicWriter.Table.NONE, ValueStream.Metadata.empty(), true);
    }

    /** A column that stores its values as they were written. */
    static Plain plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long numNullSlots,
        MonotonicWriter.Table valueAddresses,
        MonotonicWriter.Table nullSlots,
        ValueStream.Metadata values,
        boolean valuesSorted
    ) {
        return new Plain(
            iterator,
            numDocsWithField,
            numValues,
            numNullSlots,
            values.valueBytes(),
            valueAddresses,
            nullSlots,
            values,
            valuesSorted,
            null
        );
    }

    /** A column that names its values with ordinals into {@code dictionary}. */
    static Dictionary dictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long numNullSlots,
        long valueBytes,
        MonotonicWriter.Table valueAddresses,
        ValueStream.Metadata dictionary,
        NumericColumnMetadata ordinals,
        ValueStream.Metadata escapes,
        MonotonicWriter.Table escapeRanks,
        int dictionarySize,
        boolean valuesSorted
    ) {
        return new Dictionary(
            iterator,
            numDocsWithField,
            numValues,
            numNullSlots,
            valueBytes,
            valueAddresses,
            dictionary,
            ordinals,
            escapes,
            escapeRanks,
            dictionarySize,
            valuesSorted,
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
        out.writeVLong(numNullSlots());
        out.writeVLong(valueBytes());
        out.writeByte(valuesSorted() ? SORTED : NOT_SORTED);
        // Written ahead of the layout because finding a document's slots is the same question whichever
        // layout follows, and gated on counts already on the wire above. How the nulls among those slots are
        // recorded is not shared, so that goes in the body.
        if (hasValueAddresses()) {
            writeTable(out, valueAddresses());
        }
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
        long numNullSlots = in.readVLong();
        long valueBytes = in.readVLong();
        boolean valuesSorted = in.readByte() == SORTED;
        MonotonicWriter.Table valueAddresses = numValues != numDocsWithField ? readTable(in) : MonotonicWriter.Table.NONE;
        StringColumnLayout layout = StringColumnLayout.fromId(in.readByte());
        final StringColumnMetadata column = switch (layout) {
            case PLAIN -> {
                final ValueStream.Metadata values = ValueStream.Metadata.readFrom(in);
                final MonotonicWriter.Table nullSlots = numNullSlots > 0 ? readTable(in) : MonotonicWriter.Table.NONE;
                yield plain(iterator, numDocsWithField, numValues, numNullSlots, valueAddresses, nullSlots, values, valuesSorted);
            }
            case DICTIONARY -> {
                final int dictionarySize = in.readVInt();
                final ValueStream.Metadata dictionary = ValueStream.Metadata.readFrom(in);
                final NumericColumnMetadata ordinals = NumericColumnMetadata.readFrom(in, maxDoc, formatVersion);
                final ValueStream.Metadata escapes = ValueStream.Metadata.readFrom(in);
                MonotonicWriter.Table escapeRanks = escapes.numValues() > 0 ? readTable(in) : MonotonicWriter.Table.NONE;
                yield dictionary(
                    iterator,
                    numDocsWithField,
                    numValues,
                    numNullSlots,
                    valueBytes,
                    valueAddresses,
                    dictionary,
                    ordinals,
                    escapes,
                    escapeRanks,
                    dictionarySize,
                    valuesSorted
                );
            }
        };
        if (in.readByte() == 0) {
            return column;
        }
        final ValueStream.Metadata summaryTerms = in.readByte() == 0 ? null : ValueStream.Metadata.readFrom(in);
        return column.withSummary(new Summary(summaryTerms, in.readVLong(), in.readVLong(), in.readVLong()));
    }

    private static void writeTable(DataOutput out, MonotonicWriter.Table table) throws IOException {
        out.writeVLong(table.dataOffset());
        out.writeVLong(table.dataLength());
        out.writeVInt(table.meta().length);
        out.writeBytes(table.meta(), 0, table.meta().length);
    }

    private static MonotonicWriter.Table readTable(DataInput in) throws IOException {
        final long dataOffset = in.readVLong();
        final long dataLength = in.readVLong();
        final byte[] meta = new byte[in.readVInt()];
        in.readBytes(meta, 0, meta.length);
        return new MonotonicWriter.Table(dataOffset, dataLength, meta);
    }
}
