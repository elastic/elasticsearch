/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;

import java.io.IOException;

/**
 * Writes a string column. Values are written in the order the {@link StringColumnValues} cursor yields them and
 * are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: the values go into a {@link ValueStream}, which streams
 * them a block at a time and writes its offset table to a temporary file. Blocks address a fixed count of
 * values while chunks bound how many bytes are compressed at once, so a block of long urls and a block of
 * single characters are the same count of values and nothing like the same amount of data.
 *
 * <p>Only {@link StringColumnLayout#PLAIN} is written today; the layout is recorded so an ordinal layout can
 * arrive as a new id. See {@code docs/PLAN.md} for how that decision is meant to be reached.
 */
public final class StringColumnWriter {

    /**
     * Ordinals are packed a block at a time to the width the block needs, so this is how far a run of
     * narrow ordinals has to reach before a single wide one stops widening it.
     */
    private static final int ORDINAL_BLOCK_SIZE = 128;

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code data}: iterator metadata, the block-encoded values, then the block
     * offset table; returns the metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value; called
     *                         once for the iterator and once for the values
     * @param valuesPerBlock   values behind one offset in the byte stream
     * @param chunkCodec       how a chunk of the byte stream is compressed
     * @param targetChunkBytes bytes a chunk holds before it is closed
     * @param directory        directory used for the temporary table file
     * @param context          IO context for the temporary table file
     * @param data             data output (iterator, value blocks, and the offset table are appended)
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        int valuesPerBlock,
        ChunkCodec chunkCodec,
        int targetChunkBytes,
        DictionaryPolicy policy,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(iterator);
        }

        if (policy.enabled()) {
            final Vocabulary.Terms vocabulary = Vocabulary.survey(cursors.get(), policy, numValues);
            // Only a vocabulary that names every value is written today: with nowhere for an unknown value
            // to go, a term the survey missed would have no ordinal to take. An escape carries that case,
            // and follows.
            if (vocabulary != null
                && vocabulary.complete()
                && policy.worthKeeping(1.0, vocabulary.dictionaryBytes(), vocabulary.columnBytes())) {
                return writeDictionary(
                    iterator,
                    numDocsWithField,
                    numValues,
                    cursors,
                    vocabulary,
                    valuesPerBlock,
                    targetChunkBytes,
                    directory,
                    context,
                    data
                );
            }
        }

        final ValueStream.Metadata written;
        try (
            ValueStream.Writer stream = new ValueStream.Writer(
                chunkCodec,
                targetChunkBytes,
                valuesPerBlock,
                numValues,
                directory,
                context,
                data.getName(),
                data
            )
        ) {
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    stream.add(values.nextValue());
                }
            }
            written = stream.finish();
        }
        return StringColumnMetadata.plain(iterator, numDocsWithField, numValues, written);
    }

    /**
     * Writes the dictionary and an ordinal per value. Every value is named by a term, so nothing is stored
     * twice: the values stream holds nothing and the column's bytes are the terms plus the ordinals.
     */
    private static StringColumnMetadata writeDictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        Vocabulary.Terms vocabulary,
        int valuesPerBlock,
        int targetChunkBytes,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        final int dictionarySize = vocabulary.size();
        final BytesRef scratch = new BytesRef();

        final ValueStream.Metadata dictionary;
        try (
            // Read by ordinal, so consecutive reads land anywhere in it. Compressing it would mean
            // decompressing a chunk for nearly every value read, to save a few tens of kilobytes: the
            // dictionary is bounded by the policy however large the column is.
            ValueStream.Writer writer = new ValueStream.Writer(
                ChunkCodec.IDENTITY,
                targetChunkBytes,
                valuesPerBlock,
                dictionarySize,
                directory,
                context,
                data.getName(),
                data
            )
        ) {
            for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
                vocabulary.terms().get(vocabulary.sortedIds()[ordinal], scratch);
                writer.add(scratch);
            }
            dictionary = writer.finish();
        }

        final NumericColumnMetadata ordinals = NumericColumnWriter.write(
            numDocsWithField,
            numDocsWithField,
            numValues,
            () -> ordinalCursor(cursors.get(), vocabulary),
            NumericPipeline.defaultPipeline(ORDINAL_BLOCK_SIZE),
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            null,
            directory,
            context,
            data
        );
        return StringColumnMetadata.dictionary(iterator, numDocsWithField, numValues, dictionary, ordinals, dictionarySize);
    }

    /** The values of {@code source}, as the ordinals their terms take, so they can be written as a numeric column. */
    private static NumericColumnValues ordinalCursor(StringColumnValues source, Vocabulary.Terms vocabulary) {
        return new NumericColumnValues() {
            @Override
            public int valueCount() {
                return source.valueCount();
            }

            @Override
            public long nextValue() throws IOException {
                final int id = vocabulary.terms().find(source.nextValue());
                assert id >= 0 && vocabulary.ordinalOfId()[id] != Vocabulary.DROPPED
                    : "a complete vocabulary named every value, but this one is missing";
                return vocabulary.ordinalOfId()[id];
            }

            @Override
            public int docID() {
                return source.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return source.nextDoc();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return source.cost();
            }
        };
    }

}
