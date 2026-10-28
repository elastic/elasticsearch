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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.columnar.numeric.NumericBlockEncoder;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.ColumnOutputs;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes a string column — single- or multi-valued. Slots are written in the order the
 * {@link StringColumnValues} cursor yields them and are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: values and tables go into their files as they arrive,
 * and only a dictionary column stages its ordinals and escapes.
 * Blocks count values while chunks bound how many bytes are compressed at once.
 *
 * <p>Which {@link StringColumnLayout} a column takes is decided from its values: a dictionary when the terms
 * it repeats are worth naming under the caller's {@link DictionaryPolicy}, and otherwise the values
 * themselves. Where a document's slots begin is the same question either way, so both layouts write that
 * table identically. Which of those slots are null is not: a dictionary names a null with a reserved ordinal,
 * while a plain column, having no spare byte string to mean null with, stores it as a length of its own.
 */
public final class StringColumnWriter {

    /** Blocks of ordinals read to decide whether compressing them pays, so the trial costs a block or two. */
    private static final int ORDINAL_TRIAL_BLOCKS = 2;

    /** How much of what packing them costs compressing them has to save before the larger block is worth it. */
    private static final double ORDINAL_TRIAL_GAIN = 0.5;

    /** Ordinals of an {@link OrdinalRun} read at a time; a read stops at the end of a source block anyway. */
    private static final int RUN_ORDINALS = 512;
    /** Bytes of staged ordinals read back at a time when they are replayed a block at a time. */
    private static final int STAGED_CHUNK_BYTES = 16 * 1024;

    /** Terms to a block in the dictionary, so the stream records an offset for every term. */
    private static final int TERMS_PER_BLOCK = 1;

    /**
     * What closes a chunk of the dictionary. The terms are stored as they are and each is read where it lies,
     * so nothing decompresses a chunk to reach one and only the size of the chunk index is at stake.
     */
    private static final ChunkBounds DICTIONARY_CHUNKS = ChunkBounds.ofBytes(32 * 1024);

    /** The {@link InfoStream} component the writer reports copied chunks under. */
    public static final String INFO_STREAM_COMPONENT = "COLUMNAR";

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code outputs}: iterator metadata, then the values in whichever layout the
     * column takes; returns the metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc                    documents in the segment
     * @param totals                    the documents holding a slot, the slots, the null slots and the shortest
     *                                  and longest value; a plain column whose values all have one length and
     *                                  none of them null stores no lengths at all
     * @param cursors                   supplies fresh forward cursors over the documents that have a slot; for
     *                                  a sparse column needing a survey, this is called once for the combined
     *                                  iterator-and-survey pass; for all other columns it is called once per
     *                                  remaining pass (values, and on the dictionary path the ordinals)
     * @param options                   how the column is written: its dictionary policy, its chunk codec and
     *                                  the units its streams are sized in
     * @param precomputedVocabulary     a vocabulary already worked out for these values, or null to survey
     *                                  them
     * @param directory                 directory a dictionary column stages its ordinals and escapes in
     * @param context                   IO context for those staged files
     * @param outputs                   values to its data, per-document tables to its addressing, per-block
     *                                  and per-chunk tables to its navigation
     * @param skipIndex                 where a dictionary column without escapes writes the ordinal bounds of
     *                                  its intervals
     */
    public static StringColumnMetadata write(
        int maxDoc,
        StringColumnValues.Totals totals,
        IOSupplier<StringColumnValues> cursors,
        StringColumnOptions options,
        Vocabulary.Terms precomputedVocabulary,
        Directory directory,
        IOContext context,
        ColumnOutputs outputs,
        IndexOutput skipIndex
    ) throws IOException {
        return write(maxDoc, totals, cursors, options, precomputedVocabulary, directory, context, outputs, skipIndex, InfoStream.NO_OUTPUT);
    }

    /**
     * As {@link #write(int, StringColumnValues.Totals, IOSupplier, StringColumnOptions, Vocabulary.Terms, Directory,
     * IOContext, ColumnOutputs, IndexOutput)}, reporting to {@code infoStream} under {@link #INFO_STREAM_COMPONENT}
     * each run of slots whose chunks a plain column copied as they were stored ({@link StringColumnValues#plainRun()}),
     * and, once for a dictionary column, how many of its slots it took in runs of ordinals
     * ({@link StringColumnValues#ordinalRun()}).
     */
    public static StringColumnMetadata write(
        int maxDoc,
        StringColumnValues.Totals totals,
        IOSupplier<StringColumnValues> cursors,
        StringColumnOptions options,
        Vocabulary.Terms precomputedVocabulary,
        Directory directory,
        IOContext context,
        ColumnOutputs outputs,
        IndexOutput skipIndex,
        InfoStream infoStream
    ) throws IOException {
        final int numDocsWithField = totals.numDocsWithField();
        final long numValues = totals.numValues();
        final long numNullSlots = totals.numNullSlots();
        final DictionaryPolicy dictionaryPolicy = options.dictionary();
        final SummaryPolicy summaryPolicy = options.summary();
        final ChunkCodec chunkCodec = options.chunkCodec();
        final StringColumnOptions.Sizes sizes = options.sizes();
        final int valuesPerBlock = sizes.valuesPerBlock();
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(ColumnIteratorWriter.write(cursors, 0, maxDoc, outputs.addressing()));
        }

        // For a sparse column that needs a survey, the presence pass and the survey walk the same documents,
        // so they can share a single cursor. SurveyingDocs wraps the cursor and feeds values to the surveyor
        // on every nextDoc() call; IndexedDISI.writeBitSet drives the walk via the default intoBitSet, which
        // loops on nextDoc(), so every document is seen exactly once.
        final boolean combinedPass = dictionaryPolicy.enabled() && precomputedVocabulary == null && numDocsWithField < maxDoc;

        final ColumnIteratorMetadata iterator;
        Vocabulary.Terms vocabulary = null;
        if (combinedPass) {
            final SurveyingDocs docs = new SurveyingDocs(
                cursors.get(),
                Vocabulary.surveyor(dictionaryPolicy, summaryPolicy),
                numDocsWithField
            );
            iterator = ColumnIteratorWriter.write(docs, numDocsWithField, maxDoc, outputs.addressing());
            vocabulary = docs.finish();
        } else {
            iterator = ColumnIteratorWriter.write(cursors, numDocsWithField, maxDoc, outputs.addressing());
            if (dictionaryPolicy.enabled()) {
                // A merge that worked out the vocabulary from what its inputs recorded does not survey again.
                vocabulary = precomputedVocabulary != null
                    ? precomputedVocabulary
                    : Vocabulary.survey(cursors.get(), dictionaryPolicy, summaryPolicy);
            }
        }

        if (dictionaryPolicy.enabled()) {
            // Coverage is a lower bound, so a column admitted here covers at least as much as it claims.
            // NOTE: a vocabulary can hold terms for a merge and none worth an ordinal here, so the size
            // check is what keeps a bar of zero from admitting an empty dictionary.
            if (vocabulary != null
                && vocabulary.dictionarySize() > 0
                && dictionaryPolicy.worthKeeping(vocabulary.coverage(), vocabulary.dictionaryBytes(), vocabulary.columnBytes())) {
                return withSummary(
                    writeDictionary(
                        iterator,
                        totals,
                        cursors,
                        vocabulary,
                        vocabulary.columnBytes(),
                        chunkCodec,
                        sizes,
                        directory,
                        context,
                        outputs,
                        options.enableSkipIndex() ? skipIndex : null,
                        infoStream
                    ),
                    vocabulary,
                    summaryPolicy,
                    numValues - numNullSlots,
                    chunkCodec,
                    sizes,
                    outputs
                );
            }
        }

        // Set false the moment a value is seen out of order; nothing after that can restore it.
        boolean sorted = true;
        // Whether a page of this column is worth naming its values. Naming costs a hash and a probe apiece and
        // buys a consumer one entry per distinct value, so it pays where equal values arrive together and buys
        // nothing where every value differs from the one before it. The stream finds those runs anyway while
        // sizing its blocks, so what a page could collapse is known without comparing anything twice. A column
        // written under no dictionary policy was told not to weigh what it repeats, and the page decides.
        final boolean valuesWorthNaming;
        final PlainValues.Metadata written;
        final SlotAddressing addressing;
        final PlainValues.Writer stream = new PlainValues.Writer(
            chunkCodec,
            sizes.plainChunks(),
            valuesPerBlock,
            sizes.lengthBlockSize(),
            numValues,
            totals.constantLength(),
            outputs
        );
        final AddressingWriter slots = AddressingWriter.open(
            numDocsWithField,
            numValues,
            totals.oneSlotADocument(),
            sizes.slotCountsBlockSize(),
            outputs
        );
        long nulls = 0;
        long valueAddress = 0;
        final StringColumnValues values = cursors.get();
        // No value is held whole when every input can hand its values over a piece at a time and none of them is in
        // term order: the length marks a null, the cursor marks repeats, and the bytes are copied a piece at a time.
        final boolean streamed = values.streamsValues() && values.mayBeSorted() == false;
        if (streamed) {
            sorted = false;
        }
        // Whether the values arrive in term order, which lets a search bisect them instead of comparing
        // every one. Free to know here: the values are already in hand, and the comparison is one memcmp.
        // The first value out of order settles it, and the rest are written without being compared: what
        // the comparison decides cannot be restored, and the value it would compare against is not kept.
        final BytesRefBuilder previous = new BytesRefBuilder();
        boolean hasPrevious = false;
        // A run of slots whose chunks were copied ends at the first of these; a run the stream turned down ends at
        // the second, and none is asked for before it: a run is as long as it can be, so one starting inside it
        // would only be shorter.
        long copiedUntil = 0;
        long decidedUntil = 0;
        final BytesRef scratch = new BytesRef();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            slots.startDocument(valueAddress);
            if (valueAddress < copiedUntil) {
                // Its slots are already written; only where they begin is recorded.
                valueAddress += values.valueCount();
                continue;
            }
            if (valueAddress >= decidedUntil) {
                final PlainRun run = values.plainRun();
                if (run != null) {
                    decidedUntil = valueAddress + run.slots();
                    final PlainValues.Copied copied = stream.copy(run.source(), run.from(), run.to());
                    if (copied != null) {
                        copiedUntil = decidedUntil;
                        nulls += copied.nulls();
                        if (sorted) {
                            // A column in term order holds no null, so the run's first slot is its smallest value
                            // and its last the largest; only where it meets the values before it is compared.
                            if (run.sourceSorted() == false || copied.nulls() > 0) {
                                sorted = false;
                            } else {
                                run.source().get(run.from(), scratch);
                                if (hasPrevious && previous.get().compareTo(scratch) > 0) {
                                    sorted = false;
                                } else {
                                    run.source().get(run.to() - 1, scratch);
                                    previous.copyBytes(scratch);
                                    hasPrevious = true;
                                }
                            }
                        }
                        if (infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
                            infoStream.message(
                                INFO_STREAM_COMPONENT,
                                "copied ["
                                    + copied.slots()
                                    + "] plain slots, ["
                                    + copied.chunks()
                                    + "] chunks of ["
                                    + copied.bytes()
                                    + "] bytes as stored"
                            );
                        }
                        valueAddress += values.valueCount();
                        continue;
                    }
                }
            }
            for (int i = 0, count = values.valueCount(); i < count; i++) {
                values.nextValue();
                if (streamed) {
                    final int length = values.valueLength();
                    if (length < 0) {
                        nulls++;
                        stream.addNull();
                    } else {
                        stream.transfer(length, values.repeatsPrevious(), values);
                    }
                } else {
                    final BytesRef value = values.value();
                    if (value == null) {
                        // A null takes an address like any other and stores no bytes; its length is what tells it
                        // from an empty string. It has no place in term order either, so a column holding one is
                        // not one a search can bisect.
                        sorted = false;
                        nulls++;
                        stream.addNull();
                    } else {
                        if (sorted) {
                            if (hasPrevious && previous.get().compareTo(value) > 0) {
                                sorted = false;
                            } else {
                                previous.copyBytes(value);
                                hasPrevious = true;
                            }
                        }
                        stream.add(value);
                    }
                }
                valueAddress++;
            }
        }
        written = stream.finish();
        valuesWorthNaming = dictionaryPolicy.enabled() == false || stream.runs() * StringColumnReader.MIN_PAGE_REPEAT <= numValues;
        addressing = slots.finish(valueAddress);
        // Checked rather than asserted: these are on the wire, and a reader, or the next merge, would trust them.
        if (nulls != numNullSlots) {
            throw new IllegalStateException("wrote " + nulls + " null slots, counted " + numNullSlots);
        }
        if (stream.minLength() != totals.minLength() || stream.maxLength() != totals.maxLength()) {
            throw new IllegalStateException(
                "wrote values of ["
                    + stream.minLength()
                    + ", "
                    + stream.maxLength()
                    + "] bytes, counted ["
                    + totals.minLength()
                    + ", "
                    + totals.maxLength()
                    + "]"
            );
        }

        return withSummary(
            StringColumnMetadata.plain(
                iterator,
                numDocsWithField,
                numValues,
                numNullSlots,
                stream.valueBytes(),
                stream.minLength(),
                stream.maxLength(),
                addressing,
                written,
                sorted,
                valuesWorthNaming
            ),
            vocabulary,
            summaryPolicy,
            numValues - numNullSlots,
            chunkCodec,
            sizes,
            outputs
        );
    }

    /**
     * Writes the terms the survey summarised and how often it saw them, so a merge can work out a
     * vocabulary without reading this segment's values again. A column that stayed plain leaves one too:
     * the survey already ran, and the segment it merges into may be worth a dictionary where this one was
     * not.
     *
     * <p>What it writes is not this column's dictionary but every term within the vocabulary policy's
     * bound, because a term held once here may be one the merged column holds many times. Where the two
     * coincide, a dictionary column's terms are already on disk and only the counts are added.
     */
    private static StringColumnMetadata withSummary(
        StringColumnMetadata metadata,
        Vocabulary.Terms vocabulary,
        SummaryPolicy summaryPolicy,
        long nonNullValueCount,
        ChunkCodec chunkCodec,
        StringColumnOptions.Sizes sizes,
        ColumnOutputs outputs
    ) throws IOException {
        if (vocabulary == null || vocabulary.hasCounts() == false || summaryPolicy.enabled() == false) {
            return metadata;
        }
        final boolean dictionaryHoldsTheTerms = metadata instanceof StringColumnMetadata.Dictionary column
            && vocabulary.summaryIsDictionary()
            && column.dictionarySize() == vocabulary.summarySize();
        return metadata.withSummary(
            SummaryFormat.write(vocabulary, dictionaryHoldsTheTerms, nonNullValueCount, chunkCodec, sizes, outputs)
        );
    }

    /**
     * Writes the dictionary, an ordinal per value, and the values no term names.
     *
     * <p>Both are staged in temporary files first: the escapes because a stream has to be told its length
     * before it starts and how many escape is not known until the pass is over, the ordinals because the
     * numeric column reads its input more than once and a second pass would look every term up again.
     */
    private static StringColumnMetadata writeDictionary(
        ColumnIteratorMetadata iterator,
        StringColumnValues.Totals totals,
        IOSupplier<StringColumnValues> cursors,
        Vocabulary.Terms vocabulary,
        long valueBytes,
        ChunkCodec chunkCodec,
        StringColumnOptions.Sizes sizes,
        Directory directory,
        IOContext context,
        ColumnOutputs outputs,
        IndexOutput skipIndex,
        InfoStream infoStream
    ) throws IOException {
        final int numDocsWithField = totals.numDocsWithField();
        final long numValues = totals.numValues();
        final long numNullSlots = totals.numNullSlots();
        final IndexOutput data = outputs.data();
        final int escapeRankBlockSize = sizes.escapeRankBlockSize();
        final int dictionarySize = vocabulary.dictionarySize();
        // The terms start above the reserved null, and the escape marker sits one past the last of them.
        final int escapeOrdinal = dictionarySize + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
        final BytesRef scratch = new BytesRef();
        // The dictionary is in term order, so ordinals rise exactly as values do. An escaped value has no
        // ordinal to place among them, so a column that lets anything escape is not called sorted.
        boolean sorted = true;
        int previousOrdinalSeen = -1;

        final ValueStream.Metadata dictionary;
        // The length of every term by its ordinal, so a value carried over as an ordinal is priced without
        // resolving its bytes.
        final int[] termLengths = new int[dictionarySize];
        // Read by ordinal, so consecutive reads land anywhere in it. Compressing it would mean decompressing a
        // chunk for nearly every value read, to save a few tens of kilobytes: the dictionary is bounded by the
        // policy however large the column is.
        // One term to a block, so the stream keeps an offset for each of them and a term is read where it
        // lies. The offsets are a monotonic table, read off the mapped file.
        final ValueStream.Writer dictionaryWriter = new ValueStream.Writer(
            ChunkCodec.IDENTITY,
            DICTIONARY_CHUNKS,
            TERMS_PER_BLOCK,
            outputs
        );
        for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
            vocabulary.terms().get(vocabulary.dictionaryIds()[ordinal], scratch);
            termLengths[ordinal] = scratch.length;
            dictionaryWriter.add(scratch);
        }
        dictionary = dictionaryWriter.finish();

        // Document i holds slot i, so replaying the staged ordinals needs only to count the documents, not walk them.
        final boolean slotIsDocument = iterator.isDense() && totals.oneSlotADocument();
        String ordinalTempName = null;
        String escapeTempName = null;
        final List<IndexInput> replays = new ArrayList<>();
        try {
            long escapes = 0;
            // The shortest and longest value written, checked against the totals below.
            int minLength = Integer.MAX_VALUE;
            int maxLength = -1;
            // The slots actually written, handed to the addressing table so it can check the total it was given.
            long index = 0;
            final ValueStream.Metadata escapeStream;
            final MonotonicWriter.Table escapeRanks;
            final SlotAddressing addressing;
            final MonotonicWriter ranks = new MonotonicWriter(outputs.navigation());
            // Nulls are named by a reserved ordinal below, so this layout keeps no null-slot table.
            final AddressingWriter slots = AddressingWriter.open(
                numDocsWithField,
                numValues,
                totals.oneSlotADocument(),
                sizes.slotCountsBlockSize(),
                outputs
            );
            // Opened one at a time, each named before the next is asked for: a temporary file that the
            // one after it fails to open is still a file to delete, and only its name says which.
            try (IndexOutput ordinalTemp = directory.createTempOutput(data.getName(), "columnar-ordinals", context)) {
                ordinalTempName = ordinalTemp.getName();
                try (IndexOutput escapeTemp = directory.createTempOutput(data.getName(), "columnar-escapes", context)) {
                    escapeTempName = escapeTemp.getName();
                    final StringColumnValues values = cursors.get();
                    // As in the survey: a column in term order repeats each value, so the ordinal is almost
                    // always the one before it. An escaped value still has its bytes staged individually.
                    final BytesRefBuilder previous = new BytesRefBuilder();
                    int previousOrdinal = Vocabulary.DROPPED;
                    boolean hasPrevious = false;
                    // A run of slots whose ordinals are read a block at a time is staged whole when it is found, and
                    // ends here; the documents it covers are then only stepped through, so where their slots begin is
                    // recorded. None is asked for before it ends: a run is as long as it can be, so one starting inside
                    // it would only be shorter.
                    long stagedUntil = 0;
                    final int[] runOrdinals = new int[RUN_ORDINALS];
                    long runs = 0;
                    long runSlots = 0;
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        assert slotIsDocument == false || doc == index : "document [" + doc + "] holds slot [" + index + "]";
                        slots.startDocument(index);
                        if (index < stagedUntil) {
                            index += values.valueCount();
                            continue;
                        }
                        final OrdinalRun run = values.ordinalRun();
                        if (run != null) {
                            stagedUntil = index + run.slots();
                            runs++;
                            runSlots += run.slots();
                            long slot = index;
                            for (int n = run.read(runOrdinals); n > 0; n = run.read(runOrdinals)) {
                                // Nothing in a run escapes, so every escape rank it crosses records the same count, and
                                // they are found without testing each slot.
                                final long firstRank = (slot + escapeRankBlockSize - 1) / escapeRankBlockSize * escapeRankBlockSize;
                                for (long rank = firstRank; rank < slot + n; rank += escapeRankBlockSize) {
                                    ranks.add(escapes);
                                }
                                for (int i = 0; i < n; i++, slot++) {
                                    // Every slot of a run is a term's or a null, as the per-value path below finds a
                                    // carried-over ordinal or a null value to be.
                                    final int ordinal = runOrdinals[i];
                                    if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                                        sorted = false;
                                    } else {
                                        if (ordinal < previousOrdinalSeen) {
                                            sorted = false;
                                        }
                                        previousOrdinalSeen = ordinal;
                                        final int length = termLengths[ordinal - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL];
                                        minLength = Math.min(minLength, length);
                                        maxLength = Math.max(maxLength, length);
                                    }
                                    ordinalTemp.writeVInt(ordinal);
                                }
                            }
                            assert slot == stagedUntil : "staged up to " + slot + " of a run ending at " + stagedUntil;
                            // A slot a document makes the run's slots its documents, so there is nothing left to learn
                            // from stopping on the rest of them.
                            final long rest = run.slots() - 1;
                            if (totals.oneSlotADocument() && rest > 0 && values.skipDocuments(Math.toIntExact(rest))) {
                                slots.startDocuments(index + 1, Math.toIntExact(rest));
                                index = stagedUntil;
                                continue;
                            }
                            index += values.valueCount();
                            continue;
                        }
                        for (int i = 0, count = values.valueCount(); i < count; i++) {
                            if (index % escapeRankBlockSize == 0) {
                                ranks.add(escapes);
                            }
                            values.nextValue();
                            // A cursor that already knows the ordinal saves resolving the value's bytes
                            // only to look them up again, which is most of what merging such a column costs.
                            // It answers for terms alone, so a null still costs its bytes to recognise —
                            // which for a null is no bytes at all.
                            final int mapped = values.ordinal();
                            if (mapped >= 0) {
                                // Carried over rather than resolved, but it still says where the value sits
                                // among the terms, so the order is read from it as from any other ordinal.
                                if (mapped < previousOrdinalSeen) {
                                    sorted = false;
                                }
                                previousOrdinalSeen = mapped;
                                if (mapped != StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                                    final int length = termLengths[mapped - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL];
                                    minLength = Math.min(minLength, length);
                                    maxLength = Math.max(maxLength, length);
                                }
                                ordinalTemp.writeVInt(mapped);
                                index++;
                                continue;
                            }
                            final BytesRef value = values.value();
                            // A null is named by the reserved ordinal below the terms. It never reaches
                            // the dictionary or the escapes, so it cannot be confused with the empty term,
                            // and a column whose only unnamed values were nulls still reports no escapes —
                            // which is what a reader answering from the ordinals alone needs.
                            if (value == null) {
                                // No place in term order, so a column holding one is not one to bisect.
                                sorted = false;
                                ordinalTemp.writeVInt(StringColumnMetadata.Dictionary.NULL_ORDINAL);
                                index++;
                                continue;
                            }
                            minLength = Math.min(minLength, value.length);
                            maxLength = Math.max(maxLength, value.length);
                            final int ordinal;
                            if (hasPrevious && previous.get().bytesEquals(value)) {
                                ordinal = previousOrdinal;
                            } else {
                                final int id = vocabulary.terms().find(value);
                                // A term the survey saw can still have been dropped from the dictionary,
                                // so the ordinal is shifted only once it is known to name one, since DROPPED
                                // shifted would land on a reserved ordinal rather than staying a marker.
                                final int termOrdinal = id >= 0 ? vocabulary.ordinalOfId()[id] : Vocabulary.DROPPED;
                                ordinal = termOrdinal == Vocabulary.DROPPED
                                    ? Vocabulary.DROPPED
                                    : termOrdinal + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
                                previous.copyBytes(value);
                                previousOrdinal = ordinal;
                                hasPrevious = true;
                            }
                            if (ordinal == Vocabulary.DROPPED) {
                                sorted = false;
                                ordinalTemp.writeVInt(escapeOrdinal);
                                escapeTemp.writeVInt(value.length);
                                escapeTemp.writeBytes(value.bytes, value.offset, value.length);
                                escapes++;
                            } else {
                                if (ordinal < previousOrdinalSeen) {
                                    sorted = false;
                                }
                                previousOrdinalSeen = ordinal;
                                ordinalTemp.writeVInt(ordinal);
                            }
                            index++;
                        }
                    }
                    // One past the end, so the escapes in the last block can be counted like any other.
                    ranks.add(escapes);
                    if (runs > 0 && infoStream.isEnabled(INFO_STREAM_COMPONENT)) {
                        infoStream.message(
                            INFO_STREAM_COMPONENT,
                            "took [" + runSlots + "] of [" + numValues + "] dictionary slots in [" + runs + "] ordinal runs"
                        );
                    }
                }
            }
            addressing = slots.finish(index);
            // A column of nothing but nulls has no length to report.
            if (maxLength < 0) {
                minLength = -1;
            }
            // Checked rather than asserted: these are on the wire, and a reader, or the next merge, would trust them.
            if (minLength != totals.minLength() || maxLength != totals.maxLength()) {
                throw new IllegalStateException(
                    "wrote values of ["
                        + minLength
                        + ", "
                        + maxLength
                        + "] bytes, counted ["
                        + totals.minLength()
                        + ", "
                        + totals.maxLength()
                        + "]"
                );
            }
            escapeStream = replayEscapes(directory, escapeTempName, escapes, chunkCodec, sizes, outputs);
            escapeRanks = escapes == 0 ? MonotonicWriter.Table.NONE : ranks.finish();

            final String staged = ordinalTempName;
            // Compressing the ordinals only pays where they repeat, and it takes a larger block to reach
            // that repetition at all. A sample says which of the two shapes this column's ordinals take.
            final boolean compressOrdinals = compressionPaysForOrdinals(directory, context, staged, numValues, sizes);
            final int ordinalBlockSize = compressOrdinals ? sizes.compressedOrdinalBlockSize() : sizes.packedOrdinalBlockSize();
            // One ordinal a slot, reached by value address: nothing asks the ordinals which document a slot
            // belongs to, and the string column already tables that, so they table nothing themselves.
            // NOTE: a value that escapes has no ordinal to bound, and a column the reader bisects never scans its ordinals.
            final boolean skipOrdinals = skipIndex != null && escapes == 0 && (sorted && SlotAddressing.NONE.equals(addressing)) == false;
            final NumericColumnMetadata ordinals = NumericColumnWriter.write(numDocsWithField, numDocsWithField, numValues, false, () -> {
                final IndexInput in = directory.openInput(staged, IOContext.READONCE);
                replays.add(in);
                return slotIsDocument ? stagedOrdinals(numDocsWithField, in) : stagedOrdinals(cursors.get(), in);
            },
                compressOrdinals
                    ? NumericPipeline.compressedOrdinalPipeline(ordinalBlockSize)
                    : NumericPipeline.runsAndOutliersPipeline(ordinalBlockSize),
                BlockBytesCodec.forId(compressOrdinals ? BlockBytesCodec.ZSTD_ID : BlockBytesCodec.IDENTITY_ID),
                skipOrdinals ? SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID) : null,
                outputs,
                skipOrdinals ? skipIndex : null
            );
            return StringColumnMetadata.dictionary(
                iterator,
                numDocsWithField,
                numValues,
                numNullSlots,
                valueBytes,
                minLength,
                maxLength,
                addressing,
                dictionary,
                ordinals,
                escapeStream,
                escapeRanks,
                escapeRankBlockSize,
                dictionarySize,
                sorted
            );
        } finally {
            IOUtils.close(replays);
            IOUtils.deleteFilesIgnoringExceptions(directory, ordinalTempName, escapeTempName);
        }
    }

    /** One entry per block of values, plus one past the end. */
    static long escapeRankEntries(long numValues, int escapeRankBlockSize) {
        return (numValues + escapeRankBlockSize - 1) / escapeRankBlockSize + 1L;
    }

    /** Writes the staged escaped values, now that how many of them there are is known. */
    private static ValueStream.Metadata replayEscapes(
        Directory directory,
        String name,
        long count,
        ChunkCodec chunkCodec,
        StringColumnOptions.Sizes sizes,
        ColumnOutputs outputs
    ) throws IOException {
        if (count == 0) {
            return ValueStream.Metadata.empty();
        }
        final ValueStream.Writer writer = new ValueStream.Writer(chunkCodec, sizes.escapeChunks(), sizes.valuesPerBlock(), outputs);
        try (IndexInput staged = directory.openInput(name, IOContext.READONCE)) {
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

    /**
     * Whether storing this column's ordinals compressed beats storing them packed. Both sides are priced as
     * this column would actually write them — compressed through the minimal pipeline, packed through the
     * run and patched stages, each at its own block size — over the first {@link #ORDINAL_TRIAL_BLOCKS}
     * blocks of ordinals.
     *
     * <p>Packing is what a column falls back to, and those stages already take out the runs and the outliers
     * a compressor would have found, so a column they handle has to stay packed: the larger block costs a
     * point read the whole of it.
     */
    private static boolean compressionPaysForOrdinals(
        Directory directory,
        IOContext context,
        String staged,
        long numValues,
        StringColumnOptions.Sizes sizes
    ) throws IOException {
        final int compressedBlockSize = sizes.compressedOrdinalBlockSize();
        final int trialValues = ORDINAL_TRIAL_BLOCKS * compressedBlockSize;
        if (numValues < trialValues) {
            // Too few to hold repetition worth reaching, and too few to fill the larger block.
            return false;
        }
        final long[] sample = new long[trialValues];
        try (IndexInput in = directory.openInput(staged, IOContext.READONCE)) {
            for (int i = 0; i < trialValues; i++) {
                sample[i] = in.readVInt();
            }
        }
        final long packed = packedOrdinalBytes(sample, sizes.packedOrdinalBlockSize());
        final long compressed = compressedOrdinalBytes(sample, compressedBlockSize);
        return packed > 0 && compressed <= packed * ORDINAL_TRIAL_GAIN;
    }

    /** What {@code sample} occupies under the pipeline and block a packed column is written with. */
    private static long packedOrdinalBytes(long[] sample, int blockSize) throws IOException {
        final NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.runsAndOutliersPipeline(blockSize), blockSize);
        final long[] block = new long[blockSize];
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        for (int start = 0; start + blockSize <= sample.length; start += blockSize) {
            // The encoder transforms the block in place, so each measurement takes its own copy.
            System.arraycopy(sample, start, block, 0, blockSize);
            encoder.encode(block, blockSize, out);
        }
        return out.size();
    }

    /** What {@code sample} occupies under the pipeline, block and codec a compressed column is written with. */
    private static long compressedOrdinalBytes(long[] sample, int blockSize) throws IOException {
        final NumericBlockEncoder encoder = new NumericBlockEncoder(NumericPipeline.compressedOrdinalPipeline(blockSize), blockSize);
        final BlockBytesCodec codec = BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID);
        final long[] block = new long[blockSize];
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        for (int start = 0; start + blockSize <= sample.length; start += blockSize) {
            final int at = start;
            codec.write(o -> {
                System.arraycopy(sample, at, block, 0, blockSize);
                encoder.encode(block, blockSize, o);
            }, out);
        }
        return out.size();
    }

    /**
     * The staged ordinals of a dense column holding a slot a document, so they can be written as a numeric column
     * without walking the documents again: document {@code d} holds slot {@code d}.
     */
    private static NumericColumnValues stagedOrdinals(int numDocs, IndexInput staged) {
        return new NumericColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() throws IOException {
                return staged.readVInt();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc = doc < numDocs - 1 ? doc + 1 : DocIdSetIterator.NO_MORE_DOCS;
            }

            // The staged bytes, copied out a chunk at a time so a vint is decoded off an array rather than a byte at a time
            // off the input. A chunk is topped up before it runs short of the longest vint.
            private final byte[] chunk = new byte[STAGED_CHUNK_BYTES];
            private int pos;
            private int end;

            @Override
            public int nextValuesOfConsecutiveDocuments(long[] dst) throws IOException {
                final int read = doc == DocIdSetIterator.NO_MORE_DOCS ? 0 : Math.min(dst.length, numDocs - 1 - doc);
                for (int i = 0; i < read; i++) {
                    if (end - pos < 5) {
                        refill();
                    }
                    int b = chunk[pos++];
                    int value = b & 0x7F;
                    for (int shift = 7; b < 0; shift += 7) {
                        b = chunk[pos++];
                        value |= (b & 0x7F) << shift;
                    }
                    dst[i] = value;
                }
                doc += read;
                return read;
            }

            private void refill() throws IOException {
                final int left = end - pos;
                System.arraycopy(chunk, pos, chunk, 0, left);
                final int more = (int) Math.min(chunk.length - left, staged.length() - staged.getFilePointer());
                staged.readBytes(chunk, left, more);
                pos = 0;
                end = left + more;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return numDocs;
            }
        };
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
