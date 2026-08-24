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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Values per entry in the escape-rank table. Finding an escaped value means counting the escapes before
     * it, which this bounds to one block's worth of ordinals however many escaped.
     */
    static final int ESCAPE_RANK_BLOCK = 128;

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
        Vocabulary.Terms known,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(iterator);
        }

        Vocabulary.Terms surveyed = null;
        if (policy.enabled()) {
            // A merge that worked out the vocabulary from what its inputs recorded does not survey again.
            surveyed = known != null ? known : Vocabulary.survey(cursors.get(), policy, numValues);
            // Coverage is a lower bound, so a column admitted here covers at least as much as it claims.
            if (surveyed != null && policy.worthKeeping(surveyed.coverage(), surveyed.dictionaryBytes(), surveyed.columnBytes())) {
                return withSummary(
                    writeDictionary(
                        iterator,
                        numDocsWithField,
                        numValues,
                        cursors,
                        surveyed,
                        surveyed.columnBytes(),
                        valuesPerBlock,
                        chunkCodec,
                        targetChunkBytes,
                        directory,
                        context,
                        data
                    ),
                    surveyed,
                    numValues,
                    valuesPerBlock,
                    chunkCodec,
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
        return withSummary(
            StringColumnMetadata.plain(iterator, numDocsWithField, numValues, written),
            surveyed,
            numValues,
            valuesPerBlock,
            chunkCodec,
            targetChunkBytes,
            directory,
            context,
            data
        );
    }

    /**
     * Records the terms the survey found and how often it saw them, so a merge of this segment can work out
     * a vocabulary without reading its values again. A column that stayed plain keeps one too: the survey
     * already ran, and the segment merged into may well be worth a dictionary even where this one was not.
     *
     * <p>A dictionary column's terms are already on disk as its dictionary, so only the counts are added.
     */
    private static StringColumnMetadata withSummary(
        StringColumnMetadata metadata,
        Vocabulary.Terms vocabulary,
        long numValues,
        int valuesPerBlock,
        ChunkCodec chunkCodec,
        int targetChunkBytes,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        if (vocabulary == null || vocabulary.counted() == false || vocabulary.size() == 0) {
            return metadata;
        }
        final int size = vocabulary.size();
        ValueStream.Metadata terms = null;
        if (metadata.layout() != StringColumnLayout.DICTIONARY) {
            final BytesRef term = new BytesRef();
            try (
                ValueStream.Writer writer = new ValueStream.Writer(
                    chunkCodec,
                    targetChunkBytes,
                    valuesPerBlock,
                    size,
                    directory,
                    context,
                    data.getName(),
                    data
                )
            ) {
                for (int ordinal = 0; ordinal < size; ordinal++) {
                    vocabulary.terms().get(vocabulary.sortedIds()[ordinal], term);
                    writer.add(term);
                }
                terms = writer.finish();
            }
        } else {
            assert metadata.dictionarySize() == size : metadata.dictionarySize() + " != " + size;
        }
        final long countsOffset = data.getFilePointer();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            data.writeVLong(vocabulary.countOf(ordinal));
        }
        return metadata.withSummary(new StringColumnMetadata.Summary(terms, countsOffset, data.getFilePointer() - countsOffset, numValues));
    }

    /**
     * Writes the dictionary, an ordinal per value, and the values no term names.
     *
     * <p>Both the ordinals and the escaped values are staged in temporary files first. The escapes because
     * how many there are is not known until the pass is over — the survey's counts are lower bounds — and a
     * stream has to be told its length before it starts; the ordinals because the numeric column reads its
     * input more than once, and a second pass over the values would have to look every term up again.
     */
    private static StringColumnMetadata writeDictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        Vocabulary.Terms vocabulary,
        long valueBytes,
        int valuesPerBlock,
        ChunkCodec chunkCodec,
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

        String ordinalTempName = null;
        String exceptionTempName = null;
        final List<IndexInput> replays = new ArrayList<>();
        try {
            long escapes = 0;
            final ValueStream.Metadata exceptions;
            final MonotonicWriter.Table escapeRanks;
            try (MonotonicWriter ranks = new MonotonicWriter(directory, context, data.getName(), escapeRankEntries(numValues))) {
                try (
                    IndexOutput ordinalTemp = directory.createTempOutput(data.getName(), "columnar-ordinals", context);
                    IndexOutput exceptionTemp = directory.createTempOutput(data.getName(), "columnar-exceptions", context)
                ) {
                    ordinalTempName = ordinalTemp.getName();
                    exceptionTempName = exceptionTemp.getName();
                    final StringColumnValues values = cursors.get();
                    long index = 0;
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        for (int i = 0, count = values.valueCount(); i < count; i++) {
                            if (index % ESCAPE_RANK_BLOCK == 0) {
                                ranks.add(escapes);
                            }
                            // A cursor that already knows the ordinal saves resolving the value's bytes
                            // only to look them up again, which is most of what merging such a column costs.
                            final int mapped = values.nextOrdinal();
                            if (mapped >= 0) {
                                ordinalTemp.writeVInt(mapped);
                                index++;
                                continue;
                            }
                            final BytesRef value = values.nextValue();
                            final int id = vocabulary.terms().find(value);
                            final int ordinal = id >= 0 ? vocabulary.ordinalOfId()[id] : Vocabulary.DROPPED;
                            if (ordinal == Vocabulary.DROPPED) {
                                // The escape is one past the last ordinal, so it costs nothing to tell apart
                                // and widens the ordinals only where the dictionary was already that wide.
                                ordinalTemp.writeVInt(dictionarySize);
                                exceptionTemp.writeVInt(value.length);
                                exceptionTemp.writeBytes(value.bytes, value.offset, value.length);
                                escapes++;
                            } else {
                                ordinalTemp.writeVInt(ordinal);
                            }
                            index++;
                        }
                    }
                    // One past the end, so the escapes in the last block can be counted like any other.
                    ranks.add(escapes);
                }
                exceptions = replayExceptions(
                    directory,
                    context,
                    exceptionTempName,
                    escapes,
                    chunkCodec,
                    targetChunkBytes,
                    valuesPerBlock,
                    data
                );
                escapeRanks = escapes == 0 ? MonotonicWriter.Table.NONE : ranks.finish(data);
            }

            final String staged = ordinalTempName;
            final NumericColumnMetadata ordinals = NumericColumnWriter.write(numDocsWithField, numDocsWithField, numValues, () -> {
                final IndexInput in = directory.openInput(staged, context);
                replays.add(in);
                return stagedOrdinals(cursors.get(), in);
            },
                NumericPipeline.defaultPipeline(ORDINAL_BLOCK_SIZE),
                BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                null,
                directory,
                context,
                data
            );
            return StringColumnMetadata.dictionary(
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
        } finally {
            IOUtils.close(replays);
            IOUtils.deleteFilesIgnoringExceptions(directory, ordinalTempName, exceptionTempName);
        }
    }

    /** One entry per block of values, plus one past the end. */
    static long escapeRankEntries(long numValues) {
        return (numValues + ESCAPE_RANK_BLOCK - 1) / ESCAPE_RANK_BLOCK + 1L;
    }

    /** Writes the staged escaped values, now that how many of them there are is known. */
    private static ValueStream.Metadata replayExceptions(
        Directory directory,
        IOContext context,
        String name,
        long count,
        ChunkCodec chunkCodec,
        int targetChunkBytes,
        int valuesPerBlock,
        IndexOutput data
    ) throws IOException {
        if (count == 0) {
            return ValueStream.Metadata.empty();
        }
        try (
            IndexInput staged = directory.openInput(name, context);
            ValueStream.Writer writer = new ValueStream.Writer(
                chunkCodec,
                targetChunkBytes,
                valuesPerBlock,
                count,
                directory,
                context,
                data.getName(),
                data
            )
        ) {
            final BytesRef value = new BytesRef();
            for (long i = 0; i < count; i++) {
                final int length = staged.readVInt();
                if (value.bytes.length < length) {
                    value.bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
                }
                staged.readBytes(value.bytes, 0, length);
                value.offset = 0;
                value.length = length;
                writer.add(value);
            }
            return writer.finish();
        }
    }

    /** The staged ordinals, over the documents {@code source} walks, so they can be written as a numeric column. */
    private static NumericColumnValues stagedOrdinals(StringColumnValues source, IndexInput staged) {
        return new NumericColumnValues() {
            @Override
            public int valueCount() {
                return source.valueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return staged.readVInt();
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
