/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.string.BestCoverage;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.OrdinalRun;
import org.elasticsearch.columnar.string.PlainRun;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.columnar.string.SummaryMerger;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.columnar.string.Vocabulary;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ColumnOutputs;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Writes tagged columns onto the binary substrate; numeric types decode their {@code NumericBinaryPayload}
 * into the long column. Field metadata is flushed on {@link #close()}.
 *
 * <p><b>Merge contract.</b> {@link #mergeBinaryField} re-encodes all source segments through the
 * current writer's pipeline. There is no version-preserving merge and no mixed-version output
 * segment: a force-merge is a silent format upgrade.
 *
 * <p>The one exception is a plain string column's chunks. Where a source segment's documents land next to one
 * another in the merged segment ({@link MergeStretches}), the chunks holding their bytes are copied as they
 * are stored rather than decompressed and compressed again; the lengths around them are still re-encoded.
 */
final class ColumNARDocValuesConsumer extends DocValuesConsumer {

    private final int maxDoc;
    private final Directory directory;
    private final IOContext context;
    private final IndexOutput data;
    private final IndexOutput addressing;
    private final IndexOutput lengths;
    private final IndexOutput navigation;
    private final IndexOutput meta;
    private final IndexOutput skipIndex;
    private final ColumnOutputs outputs;
    private final List<FieldEntry> fields = new ArrayList<>();
    private final NumericPipelineSelector pipelineSelector;
    private final ColumnarFieldTypeSelector typeSelector;
    private final int blockSize;
    private final StringColumnOptionsSelector stringSelector;
    private final InfoStream infoStream;
    private final boolean copyChunksOnMerge;

    private boolean closed = false;

    private record FieldEntry(int fieldNumber, ColumnarFieldType type, boolean singleValued, ColumnMetadata metadata) {}

    ColumNARDocValuesConsumer(
        SegmentWriteState state,
        NumericPipelineSelector pipelineSelector,
        ColumnarFieldTypeSelector typeSelector,
        int blockSize,
        StringColumnOptionsSelector stringSelector,
        boolean copyChunksOnMerge
    ) throws IOException {
        this.pipelineSelector = pipelineSelector;
        this.typeSelector = typeSelector;
        this.blockSize = blockSize;
        this.stringSelector = stringSelector;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.directory = state.directory;
        this.context = state.context;
        this.infoStream = state.infoStream;
        this.copyChunksOnMerge = copyChunksOnMerge;
        boolean success = false;
        try {
            data = createOutput(state, ColumNARDocValuesFormat.DATA_EXTENSION, ColumNARDocValuesFormat.DATA_CODEC);
            addressing = createOutput(state, ColumNARDocValuesFormat.ADDRESSING_EXTENSION, ColumNARDocValuesFormat.ADDRESSING_CODEC);
            lengths = createOutput(state, ColumNARDocValuesFormat.LENGTHS_EXTENSION, ColumNARDocValuesFormat.LENGTHS_CODEC);
            navigation = createOutput(state, ColumNARDocValuesFormat.NAVIGATION_EXTENSION, ColumNARDocValuesFormat.NAVIGATION_CODEC);
            skipIndex = createOutput(state, ColumNARDocValuesFormat.SKIP_EXTENSION, ColumNARDocValuesFormat.SKIP_CODEC);
            meta = createOutput(state, ColumNARDocValuesFormat.META_EXTENSION, ColumNARDocValuesFormat.META_CODEC);
            outputs = new ColumnOutputs(data, addressing, lengths, navigation);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private static IndexOutput createOutput(SegmentWriteState state, String extension, String codec) throws IOException {
        final String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
        final IndexOutput out = state.directory.createOutput(name, state.context);
        boolean success = false;
        try {
            ColumnarCodecUtil.writeHeader(out, codec, FormatVersion.CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            success = true;
            return out;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(out);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        ColumnarFieldType type = typeSelector.select(field);
        // Exhaustive, so a column type added later is a compile error here rather than a surprise at runtime.
        switch (type) {
            case LONG, DOUBLE -> writeNumericColumn(
                field,
                type,
                () -> ColumnarNumericBinaryDocValues.decodePayloads(valuesProducer.getBinary(field))
            );
            case STRING -> {
                final StringColumnOptions options = stringSelector.select(field.name, type);
                writeStringColumn(field, type, options, () -> decodeBlobs(valuesProducer.getBinary(field), options), null);
            }
        }
    }

    /**
     * Merge: re-runs the encoder pipeline over the source segments, reading their values in bulk off
     * disk via {@link ColumnarNumericBinaryDocValues#directValues}. A fresh merge cursor
     * ({@link DocIDMerger} in merged doc order) is built per pass — count, iterator, values (the skip
     * index is built inline while the values are encoded, so it needs no pass of its own).
     */
    @Override
    public void mergeBinaryField(FieldInfo field, MergeState mergeState) throws IOException {
        ColumnarFieldType type = typeSelector.select(field);
        switch (type) {
            case LONG, DOUBLE -> writeNumericColumn(field, type, () -> numericMergeCursor(field, mergeState));
            case STRING -> {
                final StringColumnOptions options = stringSelector.select(field.name, type);
                final MergedVocabulary merged = mergedVocabulary(field, mergeState, options.dictionary(), options.summary());
                // Worked out once for the whole merge: every pass over the values builds a cursor of its own.
                final int[][] ordinalMaps = ordinalMaps(field, mergeState, merged);
                writeStringColumn(
                    field,
                    type,
                    options,
                    () -> stringMergeCursor(field, mergeState, ordinalMaps, options, copyChunksOnMerge),
                    merged.terms()
                );
            }
        }
    }

    private static NumericColumnValues numericMergeCursor(FieldInfo field, MergeState mergeState) throws IOException {
        List<ColumnMergeSub<NumericColumnValues>> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            // Our own numeric column decodes directly; a foreign source falls back to the payload. A ColumNAR string column
            // here is a type the wiring resolved against a segment that recorded another, so fail rather than misdecode.
            if (binary instanceof ColumnarStringBinaryDocValues) {
                throw new IllegalStateException(
                    "field [" + field.name + "] is merged as a numeric column but a source segment holds a string column"
                );
            }
            NumericColumnValues values = binary instanceof ColumnarNumericBinaryDocValues columnar
                ? columnar.directValues()
                : ColumnarNumericBinaryDocValues.decodePayloads(binary);
            cost += values.cost();
            subs.add(new ColumnMergeSub<>(mergeState.docMaps[i], values));
        }

        DocIDMerger<ColumnMergeSub<NumericColumnValues>> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new NumericColumnValues() {
            private ColumnMergeSub<NumericColumnValues> current;
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int valueCount() {
                return current.values.valueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    /**
     * One source segment's cursor, in merged doc order. The type parameter keeps the column's own value
     * accessors reachable through {@link #values}, which {@link DocIDMerger.Sub} itself does not expose.
     */
    private static final class ColumnMergeSub<T extends DocIdSetIterator> extends DocIDMerger.Sub {
        private final T values;
        /** Where the segment's documents land next to one another, for a column that copies what it can; or null. */
        private final MergeStretches stretches;

        ColumnMergeSub(MergeState.DocMap docMap, T values) {
            this(docMap, values, null);
        }

        ColumnMergeSub(MergeState.DocMap docMap, T values, MergeStretches stretches) {
            super(docMap);
            this.values = values;
            this.stretches = stretches;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /**
     * Where a merged column's terms came from, and what they were.
     *
     * @param ordinalMaps what each input's ordinals become in the merged column, as {@link #ordinalMap} answers it,
     *                    when the terms are the union of the inputs' dictionaries and so mapping them fell out of
     *                    building it; indexed like the inputs the vocabulary was worked out from, or null
     */
    record MergedVocabulary(Source source, Vocabulary.Terms terms, int[][] ordinalMaps) {
        MergedVocabulary(Source source, Vocabulary.Terms terms) {
            this(source, terms, null);
        }

        /** The ways of knowing a merged column's terms, in the order they are tried. */
        enum Source {
            /** Taken from the segments' own dictionaries, which name every value between them. */
            DICTIONARY_UNION,
            /** Summed from what the segments recorded surveying, when their dictionaries do not cover it. */
            COMBINED_SUMMARIES,
            /** No dictionary kept, either because the segments' records rule one out or because they cannot
             * show one, which at equal caps is as much as the values would show. */
            SUMMARY_REFUSAL,
            /** The records cannot speak: a deletion, a segment without one, or a cap tighter than the
             * dictionary's. The merged values are surveyed as a flush surveys them. */
            SURVEY
        }
    }

    private static final MergedVocabulary SURVEYED = new MergedVocabulary(MergedVocabulary.Source.SURVEY, null);

    /**
     * How a merged column's vocabulary is settled, without reading a value where that is possible.
     *
     * <p>Tried in order: the union of the segments' own dictionaries, which names every value between them;
     * what the segments summarised, whose summed counts can prove a dictionary worth keeping and whose best
     * coverage can prove none is, and which decides the layout either way while it was allowed the
     * dictionary's bytes; and the values, where deletions, a missing summary or a tighter cap leave the
     * records unable to speak.
     */
    MergedVocabulary mergedVocabulary(
        FieldInfo field,
        MergeState mergeState,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) throws IOException {
        if (dictionaryPolicy.enabled() == false) {
            return SURVEYED;
        }
        final List<Integer> segments = new ArrayList<>();
        final List<StringColumnReader> inputColumns = inputColumns(field, mergeState, segments);
        if (inputColumns == null) {
            return SURVEYED;
        }
        final MergedVocabulary merged = vocabularyFrom(inputColumns, hasDeletions(mergeState), dictionaryPolicy, summaryPolicy);
        if (merged.ordinalMaps() == null) {
            return merged;
        }
        // Indexed by the segment each input is, as the merge cursor finds them.
        final int[][] bySegment = new int[mergeState.docValuesProducers.length][];
        for (int i = 0; i < segments.size(); i++) {
            bySegment[segments.get(i)] = merged.ordinalMaps()[i];
        }
        return new MergedVocabulary(merged.source(), merged.terms(), bySegment);
    }

    /**
     * What each segment's dictionary ordinals become in the merged column, indexed by segment: taken from the
     * vocabulary where building it mapped them, and otherwise looked up term by term. Null when the merged column has
     * no vocabulary to map onto.
     */
    private static int[][] ordinalMaps(FieldInfo field, MergeState mergeState, MergedVocabulary merged) throws IOException {
        if (merged.ordinalMaps() != null) {
            return merged.ordinalMaps();
        }
        if (merged.terms() == null) {
            return null;
        }
        final int[][] maps = new int[mergeState.docValuesProducers.length][];
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            final DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            final FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            maps[i] = ordinalMap(producer.getBinary(readerField), merged.terms());
        }
        return maps;
    }

    /**
     * The same decision over columns already opened, so which way it went can be established without a merge
     * in flight.
     *
     * @param inputColumns the columns the merge reads, each one this codec wrote
     * @param hasDeletions whether any input still carries deleted documents
     */
    static MergedVocabulary vocabularyFrom(
        List<StringColumnReader> inputColumns,
        boolean hasDeletions,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) throws IOException {
        final MergedVocabulary union = unionOfDictionaries(inputColumns, dictionaryPolicy);
        if (union != null) {
            return union;
        }
        // NOTE: asked before the summaries are opened. A deleted value is still counted in what its segment
        // summarised, so nothing read here could be used, and reading it would allocate every term only to
        // throw the lot away.
        if (hasDeletions) {
            return SURVEYED;
        }
        final SummaryMerger.Decision decision = readSummaries(inputColumns, dictionaryPolicy, summaryPolicy).decide(true);
        return switch (decision.outcome()) {
            case DICTIONARY -> new MergedVocabulary(MergedVocabulary.Source.COMBINED_SUMMARIES, decision.vocabulary());
            case NO_DICTIONARY -> new MergedVocabulary(MergedVocabulary.Source.SUMMARY_REFUSAL, decision.vocabulary());
            case UNDECIDED -> SURVEYED;
        };
    }

    /**
     * The columns the inputs hold for {@code field}, skipping segments the field never appeared in, or null
     * where any input is not a column this codec wrote and so describes nothing a merge can reuse. The segment
     * each column belongs to is added to {@code segments}, in the same order.
     */
    private static List<StringColumnReader> inputColumns(FieldInfo field, MergeState mergeState, List<Integer> segments)
        throws IOException {
        final List<StringColumnReader> columns = new ArrayList<>(mergeState.docValuesProducers.length);
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            final DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            final FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            final BinaryDocValues binary = producer.getBinary(readerField);
            if ((binary instanceof ColumnarStringBinaryDocValues) == false) {
                return null;
            }
            final StringColumnReader reader = ((ColumnarStringBinaryDocValues) binary).reader();
            // A segment the field never appeared in has nothing to contribute and nothing to disagree with;
            // it must not decide the shape of the merged column.
            if (reader.numValues() > 0) {
                columns.add(reader);
                segments.add(i);
            }
        }
        return columns;
    }

    /**
     * The union of the segments' dictionaries, or null when it cannot stand for the merged column: a
     * segment without a dictionary, or one that let values escape, holds values the union would not name.
     * It is bounded by the same policy as a surveyed vocabulary, and abandoned once it exceeds it.
     *
     * <p>Every dictionary is in term order, so the union is had by merging them as sorted lists, and what each
     * input's ordinals become in it is known as each term is placed; no input's terms are looked up afterwards.
     */
    private static MergedVocabulary unionOfDictionaries(List<StringColumnReader> inputColumns, DictionaryPolicy dictionaryPolicy)
        throws IOException {
        // NOTE: asked of every input before the first term is copied. One input without a dictionary, or
        // with values that escaped it, holds values the union would not name, and finding that out on the
        // last input means having allocated every earlier input's terms for nothing.
        final DictionaryStringColumnReader[] dictionaries = new DictionaryStringColumnReader[inputColumns.size()];
        long columnBytes = 0;
        long numValues = 0;
        for (int i = 0; i < dictionaries.length; i++) {
            final StringColumnReader reader = inputColumns.get(i);
            if (reader instanceof DictionaryStringColumnReader dictionary && reader.escapeCount() == 0) {
                dictionaries[i] = dictionary;
            } else {
                return null;
            }
            columnBytes += reader.valueBytes();
            numValues += reader.numValues();
        }
        if (dictionaries.length == 0) {
            return null;
        }
        final List<BytesRef> union = new ArrayList<>();
        final int[][] ordinalMaps = sameDictionaries(dictionaries)
            ? identicalUnion(dictionaries, union, dictionaryPolicy)
            : mergedUnion(dictionaries, union, dictionaryPolicy);
        if (ordinalMaps == null || union.isEmpty()) {
            return null;
        }
        // How often each term is used is not recorded in a dictionary column, so the union carries no
        // counts. It does not need them: it names every value, and a merge of these segments always
        // prefers it to a survey.
        final Vocabulary.Terms terms = Vocabulary.known(
            union,
            columnBytes,
            1.0,
            numValues,
            BestCoverage.of(numValues, numValues, dictionaryPolicy.maxBytes()),
            null
        );
        return new MergedVocabulary(MergedVocabulary.Source.DICTIONARY_UNION, terms, ordinalMaps);
    }

    /**
     * Whether every dictionary holds the same terms, which is what a merge of segments drawn from one small
     * vocabulary mostly finds. Each is compared with the first, and a difference ends it at the first term that shows
     * it, so the dictionaries that differ, which mostly differ in size, cost little to tell apart.
     */
    private static boolean sameDictionaries(DictionaryStringColumnReader[] dictionaries) throws IOException {
        final DictionaryStringColumnReader first = dictionaries[0];
        for (int i = 1; i < dictionaries.length; i++) {
            if (dictionaries[i].dictionarySize() != first.dictionarySize()) {
                return false;
            }
        }
        final BytesRef term = new BytesRef();
        final BytesRef other = new BytesRef();
        for (int t = 0; t < first.dictionarySize(); t++) {
            first.termAt(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, term);
            for (int i = 1; i < dictionaries.length; i++) {
                if (term.bytesEquals(dictionaries[i].termAt(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, other)) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * The union of dictionaries that all hold the same terms: the first one's terms, added to {@code union}, with every
     * input's ordinals staying as they are; or null once the terms exceed the policy.
     */
    private static int[][] identicalUnion(DictionaryStringColumnReader[] dictionaries, List<BytesRef> union, DictionaryPolicy policy)
        throws IOException {
        final DictionaryStringColumnReader first = dictionaries[0];
        final BytesRef term = new BytesRef();
        long unionBytes = 0;
        for (int t = 0; t < first.dictionarySize(); t++) {
            first.termAt(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, term);
            unionBytes += term.length;
            if (unionBytes > policy.maxBytes()) {
                return null;
            }
            union.add(BytesRef.deepCopyOf(term));
        }
        // One map serves every input; nothing writes to it.
        final int[] identity = new int[first.dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL];
        for (int ordinal = 0; ordinal < identity.length; ordinal++) {
            identity[ordinal] = ordinal;
        }
        final int[][] ordinalMaps = new int[dictionaries.length][];
        Arrays.fill(ordinalMaps, identity);
        return ordinalMaps;
    }

    /**
     * The union of dictionaries that differ, merged as the sorted lists they are into {@code union}, with what each
     * input's ordinals become in it; or null once the terms exceed the policy.
     */
    private static int[][] mergedUnion(DictionaryStringColumnReader[] dictionaries, List<BytesRef> union, DictionaryPolicy policy)
        throws IOException {
        final int[][] ordinalMaps = new int[dictionaries.length][];
        final PriorityQueue<DictionaryCursor> queue = new PriorityQueue<>(dictionaries.length) {
            @Override
            protected boolean lessThan(DictionaryCursor a, DictionaryCursor b) {
                return a.term.compareTo(b.term) < 0;
            }
        };
        for (int i = 0; i < dictionaries.length; i++) {
            final int[] map = new int[dictionaries[i].dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL];
            map[StringColumnMetadata.Dictionary.NULL_ORDINAL] = StringColumnMetadata.Dictionary.NULL_ORDINAL;
            ordinalMaps[i] = map;
            final DictionaryCursor cursor = new DictionaryCursor(dictionaries[i], map);
            if (cursor.next()) {
                queue.add(cursor);
            }
        }
        long unionBytes = 0;
        while (queue.size() > 0) {
            final BytesRef term = BytesRef.deepCopyOf(queue.top().term);
            unionBytes += term.length;
            if (unionBytes > policy.maxBytes()) {
                return null;
            }
            final int ordinal = union.size() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
            union.add(term);
            // Every input holding the term is at it now: each holds a term once, in order.
            while (queue.size() > 0 && queue.top().term.bytesEquals(term)) {
                final DictionaryCursor cursor = queue.top();
                cursor.map[cursor.ordinal] = ordinal;
                if (cursor.next()) {
                    queue.updateTop();
                } else {
                    queue.pop();
                }
            }
        }
        return ordinalMaps;
    }

    /** One input's dictionary, read a term at a time in term order, and where its ordinals land in the union. */
    private static final class DictionaryCursor {
        private final DictionaryStringColumnReader dictionary;
        private final int[] map;
        /** Points at the dictionary's own bytes, valid until the next term is read. */
        private final BytesRef term = new BytesRef();
        private int ordinal = StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL - 1;

        DictionaryCursor(DictionaryStringColumnReader dictionary, int[] map) {
            this.dictionary = dictionary;
            this.map = map;
        }

        /** Moves to the next term, or answers false once the dictionary is read. */
        boolean next() throws IOException {
            if (++ordinal == dictionary.dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL) {
                return false;
            }
            dictionary.termAt(ordinal, term);
            return true;
        }
    }

    private static boolean hasDeletions(MergeState mergeState) {
        for (Bits liveDocs : mergeState.liveDocs) {
            if (liveDocs != null) {
                return true;
            }
        }
        return false;
    }

    private static SummaryMerger readSummaries(
        List<StringColumnReader> inputColumns,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) throws IOException {
        final SummaryMerger merger = new SummaryMerger(dictionaryPolicy, summaryPolicy);
        if (merger.needsSummaries() == false) {
            return merger;
        }
        // NOTE: one input short of a summary settles the merge on its own, so it is looked for on every
        // input before any of their terms are read.
        for (StringColumnReader reader : inputColumns) {
            if (reader.hasSummary() == false) {
                merger.addWithoutSummary();
                return merger;
            }
        }
        final List<BytesRef> terms = new ArrayList<>();
        final List<Long> counts = new ArrayList<>();
        for (StringColumnReader reader : inputColumns) {
            terms.clear();
            counts.clear();
            if (reader.hasSummaryTerms()) {
                reader.readSummary(terms, counts);
            }
            merger.add(reader.summaryValues(), reader.valueBytes(), reader.bestCoverage(), terms, counts);
        }
        return merger;
    }

    /**
     * What each of a segment's dictionary ordinals becomes in the merged column, indexed by the segment's own
     * ordinal, or null when the segment has no dictionary the merged vocabulary was built from.
     *
     * <p>Only the terms are mapped. A null and an escaped value both fall outside it, and the cursor answers
     * {@code -1} for either, because neither names a term the merged column would recognise: what those are
     * is settled by the value the cursor hands back, not by an ordinal meaning something else over there.
     */
    private static int[] ordinalMap(BinaryDocValues values, Vocabulary.Terms vocabulary) throws IOException {
        if (vocabulary == null || (values instanceof ColumnarStringBinaryDocValues) == false) {
            return null;
        }
        final StringColumnReader reader = ((ColumnarStringBinaryDocValues) values).reader();
        if ((reader instanceof DictionaryStringColumnReader) == false) {
            return null;
        }
        final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
        // Long enough for the term ordinals and no longer, so the escape marker indexes off the end and is
        // turned away without a test of its own. The reserved null maps to itself, for a caller that translates
        // a run of ordinals without telling the null apart first.
        final int[] map = new int[dictionary.dictionarySize() + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL];
        map[StringColumnMetadata.Dictionary.NULL_ORDINAL] = StringColumnMetadata.Dictionary.NULL_ORDINAL;
        // That "off the end" is the escape marker's own ordinal, and the two are only equal by construction.
        // Pinned here so a reserved-ordinal space that ever grew would fail rather than let an escape index a
        // real term's entry and be remapped as though the dictionary named it.
        assert map.length == dictionary.escapeOrdinal()
            : "ordinal map of " + map.length + " does not end at the escape marker " + dictionary.escapeOrdinal();
        final BytesRef term = new BytesRef();
        for (int i = 0; i < dictionary.dictionarySize(); i++) {
            final int ordinal = StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + i;
            dictionary.termAt(ordinal, term);
            final int id = vocabulary.terms().find(term);
            if (id < 0 || vocabulary.ordinalOfId()[id] == Vocabulary.DROPPED) {
                // The merged vocabulary was built from these dictionaries, so every term should be in it.
                return null;
            }
            // The ordinal the merged column will store, so the writer can take it as it stands.
            map[ordinal] = vocabulary.ordinalOfId()[id] + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
        }
        return map;
    }

    /**
     * The string counterpart of {@link #numericMergeCursor}: reads each source segment's values off disk via
     * {@link ColumnarStringBinaryDocValues#directValues}, in merged doc order. A fresh cursor is built per
     * pass - the count, the iterator, then the values.
     */
    private static StringColumnValues stringMergeCursor(
        FieldInfo field,
        MergeState mergeState,
        int[][] ordinalMaps,
        StringColumnOptions options,
        boolean copyChunks
    ) throws IOException {
        List<ColumnMergeSub<StringColumnValues>> subs = new ArrayList<>();
        long cost = 0;
        // What the counting pass would work out, summed from what the segments recorded. Held only while
        // every input is one of our own columns contributing all of its documents; anything else — a foreign
        // segment, or one with deletions — and there is nothing to sum, so the pass has to run.
        int numDocsWithField = 0;
        long numValues = 0;
        long numNullSlots = 0;
        int minLength = -1;
        int maxLength = -1;
        boolean recorded = true;
        boolean oneSlotADocument = true;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            // A ColumNAR numeric column here is a type mismatch; fail rather than misdecode.
            if (binary instanceof ColumnarNumericBinaryDocValues) {
                throw new IllegalStateException(
                    "field [" + field.name + "] is merged as a string column but a source segment holds a numeric column"
                );
            }
            // Read decoded values directly for our own columns; fall back to the payload for anything else.
            final StringColumnValues values;
            if (binary instanceof ColumnarStringBinaryDocValues columnar) {
                values = columnar.directValues(ordinalMaps == null ? null : ordinalMaps[i]);
                final StringColumnReader reader = columnar.reader();
                numDocsWithField += reader.numDocsWithField();
                numValues += reader.numValues();
                // A column keeping an addressing table has a document out of step with its slots somewhere.
                oneSlotADocument &= reader.hasValueAddresses() == false;
                numNullSlots += reader.numNullSlots();
                if (reader.minLength() >= 0) {
                    minLength = minLength < 0 ? reader.minLength() : Math.min(minLength, reader.minLength());
                    maxLength = Math.max(maxLength, reader.maxLength());
                }
                // A deleted document is dropped as the merger maps it, so the segment's totals over-state
                // what this merge takes from it.
                recorded &= mergeState.liveDocs[i] == null;
            } else {
                values = decodeBlobs(binary, options);
                recorded = false;
            }
            cost += values.cost();
            subs.add(new ColumnMergeSub<>(mergeState.docMaps[i], values, copyChunks ? new MergeStretches(mergeState, i) : null));
        }

        // Values are streamed only when every input streams. One unsorted input marks the merged column unsorted, even if
        // deletions left it in order, which only forgoes a bisection.
        boolean streams = true;
        boolean mayBeSorted = true;
        for (ColumnMergeSub<StringColumnValues> sub : subs) {
            streams &= sub.values.streamsValues();
            mayBeSorted &= sub.values.mayBeSorted();
        }
        final boolean streamsValues = streams;
        final boolean valuesMayBeSorted = mayBeSorted;
        DocIDMerger<ColumnMergeSub<StringColumnValues>> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        StringColumnValues.Totals totals = recorded
            ? new StringColumnValues.Totals(numDocsWithField, numValues, numNullSlots, minLength, maxLength, oneSlotADocument)
            : null;
        return new StringColumnValues() {
            private ColumnMergeSub<StringColumnValues> current;
            private int docID = -1;
            /** The input the previous value came from; an input's recorded repeat holds only if it is the current one. */
            private ColumnMergeSub<StringColumnValues> previousValueFrom;
            private boolean sameInputAsPrevious;

            @Override
            public Totals totals() {
                return totals;
            }

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int ordinal() throws IOException {
                return current.values.ordinal();
            }

            @Override
            public int valueCount() throws IOException {
                return current.values.valueCount();
            }

            @Override
            public int nullCount() throws IOException {
                return current.values.nullCount();
            }

            @Override
            public void nextValue() throws IOException {
                sameInputAsPrevious = previousValueFrom == current;
                previousValueFrom = current;
                current.values.nextValue();
            }

            @Override
            public BytesRef value() throws IOException {
                return current.values.value();
            }

            @Override
            public void transferValue(BytesSink sink) throws IOException {
                current.values.transferValue(sink);
            }

            @Override
            public boolean streamsValues() {
                return streamsValues;
            }

            @Override
            public boolean repeatsPrevious() throws IOException {
                return sameInputAsPrevious && current.values.repeatsPrevious();
            }

            @Override
            public boolean mayBeSorted() {
                return valuesMayBeSorted;
            }

            @Override
            public int valueLength() throws IOException {
                return current.values.valueLength();
            }

            @Override
            public PlainRun plainRun() throws IOException {
                // Only our own column's cursor knows where its slots are stored; anything else is written value
                // by value.
                if (current.stretches != null && current.values instanceof ColumnarStringBinaryDocValues.DirectValues direct) {
                    final PlainRun run = direct.plainRun(current.stretches.contiguousEnd(direct.docID()));
                    if (run != null) {
                        // A run copied is not stepped through value by value, so the value written before the next
                        // one stepped through is not known to be this input's previous.
                        previousValueFrom = null;
                    }
                    return run;
                }
                return null;
            }

            @Override
            public OrdinalRun ordinalRun() throws IOException {
                if (current.stretches != null && current.values instanceof ColumnarStringBinaryDocValues.DirectValues direct) {
                    final OrdinalRun run = direct.ordinalRun(current.stretches.contiguousEnd(direct.docID()));
                    if (run != null) {
                        // As for a plain run: the values it covers are not stepped through.
                        previousValueFrom = null;
                    }
                    return run;
                }
                return null;
            }

            @Override
            public boolean skipDocuments(int count) throws IOException {
                // The documents a run covers sit in one stretch, where no other input's document lands among them, so
                // the merger would hand back this input for each of them and the input is moved on without it.
                if (current.stretches == null || current.values.skipDocuments(count) == false) {
                    return false;
                }
                current.mappedDocID = current.docMap.get(current.values.docID());
                docID = current.mappedDocID;
                return true;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    private static StringColumnValues decodeBlobs(BinaryDocValues binary, StringColumnOptions options) {
        return options.singleValued()
            ? ColumnarStringBinaryDocValues.decodeRawValues(binary)
            : ColumnarStringBinaryDocValues.decodePayloads(binary);
    }

    private void writeNumericColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<NumericColumnValues> cursors) throws IOException {
        // Count in one pass, then stream the values block by block from fresh cursors — never buffer
        // the whole field on-heap, so a large merge stays memory-bounded.
        int numDocsWithField = 0;
        long numValues = 0;
        NumericColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            numValues += counter.valueCount();
        }

        // A BINARY field can't carry a skipper, so the column builds its own skip index inline
        // during the value-encode pass — no extra cursor over the data.
        final NumericPipeline pipeline = pipelineSelector.select(field.name, type).build(blockSize);
        assert pipeline.blockSize() == blockSize
            : "template ignored blockSize argument: built " + pipeline.blockSize() + ", expected " + blockSize;
        NumericColumnMetadata metadata = NumericColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            true,
            cursors,
            pipeline,
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
            outputs,
            skipIndex
        );
        fields.add(new FieldEntry(field.number, type, false, metadata));
    }

    /**
     * Counts the column in one pass, then streams the slots block by block from fresh cursors — never
     * buffering the whole field on-heap. The totals the pass collects are needed up front: they decide whether
     * a plain column stores lengths at all, and where its blocks of values end.
     *
     * <p>Where the column may take a dictionary and nothing has settled its terms yet, the same pass surveys
     * them: on ingest every pass copies each value out of the indexing buffer again, and the count and the
     * survey each need every value, so the two share one walk and the writer is handed both. A sparse column
     * then walks its documents once more for its presence, reading no value, and every column reads its
     * values once more to write them — two reads of each value, dense or sparse.
     *
     * <p>A cursor that already knows the totals — a merge of our own columns, which each recorded theirs —
     * reports them instead and the pass is skipped; such a merge mostly knows its terms from what the inputs
     * recorded too, and where it does not the writer surveys them itself. The writer's own checks still hold
     * the totals against what it writes, so a cursor that mis-reports them does not go unnoticed.
     *
     * @param known the terms a merge settled from what its inputs recorded, or null where the values decide
     */
    private void writeStringColumn(
        FieldInfo field,
        ColumnarFieldType type,
        StringColumnOptions options,
        IOSupplier<StringColumnValues> cursors,
        Vocabulary.Terms known
    ) throws IOException {
        StringColumnValues counter = cursors.get();
        StringColumnValues.Totals totals = counter.totals();
        Vocabulary.Survey survey = known == null ? null : new Vocabulary.Survey(known);
        if (totals == null && known == null && options.dictionary().enabled()) {
            final Vocabulary.Counted counted = Vocabulary.countAndSurvey(counter, options.dictionary(), options.summary());
            totals = counted.totals();
            survey = counted.survey();
        } else if (totals == null) {
            // Nothing to survey: the column takes no dictionary, or a merge already knows its terms and only
            // lost the totals to the deletions it drops on the way.
            int numDocsWithField = 0;
            long numValues = 0;
            long numNullSlots = 0;
            int minLength = -1;
            int maxLength = -1;
            boolean oneSlotADocument = true;
            for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
                numDocsWithField++;
                final int count = counter.valueCount();
                oneSlotADocument &= count == 1;
                numValues += count;
                numNullSlots += counter.nullCount();
                // A cursor over one of our own plain columns answers a length without resolving the value;
                // over a dictionary column it resolves the term, as writing the column does.
                for (int i = 0; i < count; i++) {
                    counter.nextValue();
                    final int length = counter.valueLength();
                    if (length >= 0) {
                        minLength = minLength < 0 ? length : Math.min(minLength, length);
                        maxLength = Math.max(maxLength, length);
                    }
                }
            }
            totals = new StringColumnValues.Totals(numDocsWithField, numValues, numNullSlots, minLength, maxLength, oneSlotADocument);
        }

        StringColumnMetadata metadata = StringColumnWriter.write(
            maxDoc,
            totals,
            cursors,
            options,
            survey,
            directory,
            context,
            outputs,
            skipIndex,
            infoStream
        );
        fields.add(new FieldEntry(field.number, type, options.singleValued(), metadata));
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("numeric");
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-numeric");
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted");
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-set");
    }

    private static UnsupportedOperationException typedNotSupported(String shape) {
        return new UnsupportedOperationException(
            "ColumNAR is a binary doc-values format and does not handle "
                + shape
                + " doc values; store the field as a binary doc-values field and supply its type via a ColumnarFieldTypeSelector"
        );
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        boolean success = false;
        try {
            for (FieldEntry entry : fields) {
                meta.writeInt(entry.fieldNumber());
                meta.writeByte(entry.type().id());
                if (entry.type() == ColumnarFieldType.STRING) {
                    meta.writeByte(entry.singleValued() ? ColumNARDocValuesFormat.PLAIN : ColumNARDocValuesFormat.COLUMNAR_PAYLOAD);
                }
                entry.metadata().writeTo(meta);
            }
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
            CodecUtil.writeFooter(data);
            CodecUtil.writeFooter(addressing);
            CodecUtil.writeFooter(lengths);
            CodecUtil.writeFooter(navigation);
            CodecUtil.writeFooter(skipIndex);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, addressing, lengths, navigation, skipIndex, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, addressing, lengths, navigation, skipIndex, meta);
            }
        }
    }
}
