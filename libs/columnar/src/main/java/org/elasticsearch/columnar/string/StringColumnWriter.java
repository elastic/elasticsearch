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
import org.elasticsearch.columnar.numeric.NumericBlockEncoder;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
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

/**
 * Writes a string column — single- or multi-valued. Slots are written in the order the
 * {@link StringColumnValues} cursor yields them and are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: values and tables go into their files as they arrive,
 * and only a dictionary column stages anything — its escapes, whose stream needs their count before it starts.
 * Blocks count values while chunks bound how many bytes are compressed at once.
 *
 * <p>Which {@link StringColumnLayout} a column takes is decided from its values: a dictionary when the terms
 * it repeats are worth naming under the caller's {@link DictionaryPolicy}, and otherwise the values
 * themselves. Where a document's slots begin is the same question either way, so both layouts write that
 * table identically. Which of those slots are null is not: a dictionary names a null with a reserved ordinal,
 * while a plain column, having no spare byte string to mean null with, stores it as a length of its own.
 *
 * <p>The values are walked as few times as the layout allows. The caller has counted them, and normally
 * surveyed them in the same walk; a sparse column then walks its documents once more for its presence, reading
 * no value; and the values are read once more to be written, and once more again where a dictionary column
 * replays its staged ordinals. Only a caller that could not settle the terms —
 * a merge whose inputs recorded nothing it can use — has them surveyed here, and a sparse column folds that
 * survey into its presence walk.
 */
public final class StringColumnWriter {

    /** Blocks of ordinals read to decide whether compressing them pays, so the trial costs a block or two. */
    private static final int ORDINAL_TRIAL_BLOCKS = 2;

    /** How much of what packing them costs compressing them has to save before the larger block is worth it. */
    private static final double ORDINAL_TRIAL_GAIN = 0.5;

    /** Terms to a block in the dictionary, so the stream records an offset for every term. */
    private static final int TERMS_PER_BLOCK = 1;

    /**
     * What closes a chunk of the dictionary. The terms are stored as they are and each is read where it lies,
     * so nothing decompresses a chunk to reach one and only the size of the chunk index is at stake.
     */
    private static final ChunkBounds DICTIONARY_CHUNKS = ChunkBounds.ofBytes(32 * 1024);

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
     *                                  remaining pass (the survey, where there is one, and the values)
     * @param options                   how the column is written: its dictionary policy, its chunk codec and
     *                                  the units its streams are sized in
     * @param survey                    the column's terms as already settled — by the caller's own survey of
     *                                  these values, or by what a merge's inputs recorded — or null to survey
     *                                  them here. A survey that found nothing is still a survey: it is handed
     *                                  over with null terms, and the values are not surveyed again
     * @param directory                 directory a dictionary column with escapes stages them in
     * @param context                   IO context for that staged file
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
        Vocabulary.Survey survey,
        Directory directory,
        IOContext context,
        ColumnOutputs outputs,
        IndexOutput skipIndex
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

        // Surveyed here only when the caller has not settled the terms: a flush counts and surveys in one pass
        // before it gets here, and a merge mostly knows them from what its inputs recorded. What is left is a
        // merge whose records cannot speak, and it surveys as a flush used to.
        final boolean needsSurvey = dictionaryPolicy.enabled() && survey == null;
        // For a sparse column that still needs a survey, the presence pass and the survey walk the same
        // documents, so they share one cursor: SurveyingDocs feeds the surveyor on every nextDoc() call, and
        // IndexedDISI.writeBitSet drives the walk through the default intoBitSet, which loops on nextDoc(), so
        // every document is seen exactly once. Otherwise the presence pass reads no value at all.
        final boolean combinedPass = needsSurvey && numDocsWithField < maxDoc;

        final ColumnIteratorMetadata iterator;
        final Vocabulary.Terms vocabulary;
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
            if (needsSurvey) {
                vocabulary = Vocabulary.survey(cursors.get(), dictionaryPolicy, summaryPolicy);
            } else {
                // Settled already, possibly as nothing. A column written under no dictionary policy was told
                // not to weigh what it repeats, so whatever was settled for it is not consulted either.
                vocabulary = dictionaryPolicy.enabled() && survey != null ? survey.terms() : null;
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
                        options.enableSkipIndex() ? skipIndex : null
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
        if (values.streamsValues() && values.mayBeSorted() == false) {
            // No value is held whole: the length marks a null, the cursor marks repeats, and the bytes are copied a
            // piece at a time.
            sorted = false;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                slots.startDocument(valueAddress);
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    values.nextValue();
                    final int length = values.valueLength();
                    if (length < 0) {
                        nulls++;
                        stream.addNull();
                    } else {
                        stream.transfer(length, values.repeatsPrevious(), values);
                    }
                    valueAddress++;
                }
            }
        } else {
            // Whether the values arrive in term order, which lets a search bisect them instead of comparing
            // every one. Free to know here: the values are already in hand, and the comparison is one memcmp.
            // The first value out of order settles it, and the rest are written without being compared: what
            // the comparison decides cannot be restored, and the value it would compare against is not kept.
            final BytesRefBuilder previous = new BytesRefBuilder();
            boolean hasPrevious = false;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                slots.startDocument(valueAddress);
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    values.nextValue();
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
                    valueAddress++;
                }
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
     * <p>The ordinals stream into the data as the values are walked, through a {@link NumericColumnWriter.Stream}:
     * the column they make is dense over the documents walked and nothing reads it by document, so it needs no
     * pass of its own. Only the first {@link #ORDINAL_TRIAL_BLOCKS} blocks of them are held back, until the
     * sample says whether they compress; the rest go straight through. The escapes are the one thing staged, in
     * a temporary file created on the first of them: a stream has to be told its length before it starts, and
     * how many escape is not known until the pass is over.
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
        IndexOutput skipIndex
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

        // The escapes' temporary file, once the first value escapes; null until then, and for the common
        // column that none does, so that column creates and deletes no file at all.
        IndexOutput escapeTemp = null;
        String escapeTempName = null;
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
            // The ordinals go straight into the data: the terms are already there, and the escapes are staged,
            // so nothing else writes it until the pass is over. The skip index is fed whenever there is a file
            // for it, since whether the column keeps one is known only once the pass has found its escapes.
            // The work is wasted when the column drops the skipper at the end (sorted columns, or columns
            // with escapes), but the cost is small, so we accept it.
            final OrdinalStream ordinalStream = new OrdinalStream(
                numValues,
                sizes,
                skipIndex == null ? null : SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
                outputs
            );
            final StringColumnValues values = cursors.get();
            // As in the survey: a column in term order repeats each value, so the ordinal is almost
            // always the one before it. An escaped value still has its bytes staged individually.
            final BytesRefBuilder previous = new BytesRefBuilder();
            int previousOrdinal = Vocabulary.DROPPED;
            boolean hasPrevious = false;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                slots.startDocument(index);
                final int count = values.valueCount();
                ordinalStream.startDoc(doc, count);
                for (int i = 0; i < count; i++) {
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
                        ordinalStream.add(mapped);
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
                        ordinalStream.add(StringColumnMetadata.Dictionary.NULL_ORDINAL);
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
                        ordinalStream.add(escapeOrdinal);
                        if (escapeTemp == null) {
                            // Named as soon as it is open: a file whose write fails is still a file to delete.
                            escapeTemp = directory.createTempOutput(data.getName(), "columnar-escapes", context);
                            escapeTempName = escapeTemp.getName();
                        }
                        escapeTemp.writeVInt(value.length);
                        escapeTemp.writeBytes(value.bytes, value.offset, value.length);
                        escapes++;
                    } else {
                        if (ordinal < previousOrdinalSeen) {
                            sorted = false;
                        }
                        previousOrdinalSeen = ordinal;
                        ordinalStream.add(ordinal);
                    }
                    index++;
                }
            }
            // One past the end, so the escapes in the last block can be counted like any other.
            ranks.add(escapes);
            if (escapeTemp != null) {
                escapeTemp.close();
                escapeTemp = null;
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
            // One ordinal a slot, reached by value address: nothing asks the ordinals which document a slot
            // belongs to, and the string column already tables that, so they table nothing themselves.
            // NOTE: a value that escapes has no ordinal to bound, and a column the reader bisects never scans its ordinals.
            final boolean skipOrdinals = skipIndex != null && escapes == 0 && (sorted && SlotAddressing.NONE.equals(addressing)) == false;
            // Finished before the escapes are replayed: both write the data, and the ordinals' blocks have to
            // be whole before the escapes' chunks follow them.
            final NumericColumnMetadata ordinals = ordinalStream.finish(skipOrdinals, skipIndex);
            escapeStream = replayEscapes(directory, escapeTempName, escapes, chunkCodec, sizes, outputs);
            escapeRanks = escapes == 0 ? MonotonicWriter.Table.NONE : ranks.finish();
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
            // Still open only when the pass failed partway; closed before it is deleted either way.
            IOUtils.closeWhileHandlingException(escapeTemp);
            if (escapeTempName != null) {
                IOUtils.deleteFilesIgnoringExceptions(directory, escapeTempName);
            }
        }
    }

    /**
     * The ordinals of a dictionary column on their way into the data as a numeric column. Compressing them
     * only pays where they repeat, and it takes a larger block to reach that repetition at all, so which of the
     * two shapes they take is decided from the first {@link #ORDINAL_TRIAL_BLOCKS} blocks of them. Those are
     * held back, with the documents they belong to, until they are all in hand; then the stream is opened in
     * the shape the trial chose, the held ordinals are replayed into it, and every ordinal after goes straight
     * through. A column with fewer ordinals than the trial needs is packed, and holds nothing back.
     *
     * <p>What is held is bounded by the trial: that many ordinals, and as many documents as hold them, which is
     * no more than that unless a document holds no slot at all, and then only one entry more per such document.
     */
    private static final class OrdinalStream {

        private final StringColumnOptions.Sizes sizes;
        private final SkipIndexCodec skipCodec;
        private final ColumnOutputs outputs;

        /** The ordinals held for the trial, and the documents they came from; null once the stream is open. */
        private long[] sample;
        private int sampled;
        private int[] sampleDocs;
        private int[] sampleCounts;
        private int sampledDocs;

        private NumericColumnWriter.Stream stream;

        OrdinalStream(long numValues, StringColumnOptions.Sizes sizes, SkipIndexCodec skipCodec, ColumnOutputs outputs) {
            this.sizes = sizes;
            this.skipCodec = skipCodec;
            this.outputs = outputs;
            final int trialValues = ORDINAL_TRIAL_BLOCKS * sizes.compressedOrdinalBlockSize();
            if (numValues < trialValues) {
                // Too few to hold repetition worth reaching, and too few to fill the larger block.
                open(false);
            } else {
                sample = new long[trialValues];
                sampleDocs = new int[Math.min(trialValues, 1024)];
                sampleCounts = new int[sampleDocs.length];
            }
        }

        /** Begins document {@code doc}, whose {@code count} ordinals follow through {@link #add}. */
        void startDoc(int doc, int count) {
            if (stream != null) {
                stream.startDoc(doc, count);
                return;
            }
            if (sampledDocs == sampleDocs.length) {
                sampleDocs = ArrayUtil.grow(sampleDocs, sampledDocs + 1);
                sampleCounts = ArrayUtil.growExact(sampleCounts, sampleDocs.length);
            }
            sampleDocs[sampledDocs] = doc;
            sampleCounts[sampledDocs] = count;
            sampledDocs++;
        }

        /** The next ordinal of the current document. */
        void add(int ordinal) throws IOException {
            if (stream != null) {
                stream.add(ordinal);
                return;
            }
            sample[sampled++] = ordinal;
            if (sampled == sample.length) {
                open(compressionPaysForOrdinals(sample, sizes));
                replay();
            }
        }

        /**
         * Finishes the ordinals and answers their column.
         *
         * @see NumericColumnWriter.Stream#finish
         */
        NumericColumnMetadata finish(boolean writeSkipper, IndexOutput skipIndex) throws IOException {
            // The stream is open already: a column below the trial size opens it in the constructor, and any
            // other column fills the trial because the slot count was checked against numValues before this.
            // Fail here rather than silently write packed ordinals for a column that came up short.
            assert stream != null : "ordinal stream finished before the trial ended, only [" + sampled + "] ordinals arrived";
            return stream.finish(writeSkipper, skipIndex);
        }

        private void open(boolean compressOrdinals) {
            final int ordinalBlockSize = compressOrdinals ? sizes.compressedOrdinalBlockSize() : sizes.packedOrdinalBlockSize();
            stream = new NumericColumnWriter.Stream(
                compressOrdinals
                    ? NumericPipeline.compressedOrdinalPipeline(ordinalBlockSize)
                    : NumericPipeline.runsAndOutliersPipeline(ordinalBlockSize),
                BlockBytesCodec.forId(compressOrdinals ? BlockBytesCodec.ZSTD_ID : BlockBytesCodec.IDENTITY_ID),
                skipCodec,
                outputs
            );
        }

        /**
         * Hands what was held to the stream, document by document. The last document held may have more
         * ordinals still to come: it is begun with its whole count, and they follow through {@link #add}.
         */
        private void replay() throws IOException {
            int at = 0;
            for (int d = 0; d < sampledDocs; d++) {
                stream.startDoc(sampleDocs[d], sampleCounts[d]);
                for (int i = 0; i < sampleCounts[d] && at < sampled; i++) {
                    stream.add(sample[at++]);
                }
            }
            sample = null;
            sampleDocs = null;
            sampleCounts = null;
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
     * run and patched stages, each at its own block size — over {@code sample}, the first
     * {@link #ORDINAL_TRIAL_BLOCKS} blocks of ordinals.
     *
     * <p>Packing is what a column falls back to, and those stages already take out the runs and the outliers
     * a compressor would have found, so a column they handle has to stay packed: the larger block costs a
     * point read the whole of it.
     */
    private static boolean compressionPaysForOrdinals(long[] sample, StringColumnOptions.Sizes sizes) throws IOException {
        final long packed = packedOrdinalBytes(sample, sizes.packedOrdinalBlockSize());
        final long compressed = compressedOrdinalBytes(sample, sizes.compressedOrdinalBlockSize());
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

}
