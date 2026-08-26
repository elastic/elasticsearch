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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.columnar.string.ValueStream;
import org.elasticsearch.columnar.string.Vocabulary;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Writes tagged columns onto the binary substrate; numeric types decode their {@code NumericBinaryPayload}
 * into the long column. Field metadata is flushed on {@link #close()}.
 *
 * <p><b>Merge contract.</b> {@link #mergeBinaryField} re-encodes all source segments through the
 * current writer's pipeline. There is no version-preserving merge and no mixed-version output
 * segment: a force-merge is a silent format upgrade.
 */
final class ColumNARDocValuesConsumer extends DocValuesConsumer {

    private final int maxDoc;
    private final Directory directory;
    private final IOContext context;
    private final IndexOutput data;
    private final IndexOutput meta;
    private final List<FieldEntry> fields = new ArrayList<>();
    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;
    private final DictionaryPolicy dictionaryPolicy;

    /** Bytes a chunk of a string column's byte stream holds before it is closed and compressed. */
    private static final int TARGET_CHUNK_BYTES = 64 * 1024;
    private boolean closed = false;

    private record FieldEntry(int fieldNumber, byte fieldTypeId, ColumnMetadata metadata) {}

    ColumNARDocValuesConsumer(
        SegmentWriteState state,
        NumericPipelineSelector pipelineSelector,
        int blockSize,
        DictionaryPolicy dictionaryPolicy
    ) throws IOException {
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.dictionaryPolicy = dictionaryPolicy;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.directory = state.directory;
        this.context = state.context;
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            data = state.directory.createOutput(dataName, state.context);
            ColumnarCodecUtil.writeHeader(
                data,
                ColumNARDocValuesFormat.DATA_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.META_EXTENSION
            );
            meta = state.directory.createOutput(metaName, state.context);
            ColumnarCodecUtil.writeHeader(
                meta,
                ColumNARDocValuesFormat.META_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        // Exhaustive, so a column type added later is a compile error here rather than a surprise at runtime.
        switch (type) {
            case LONG, DOUBLE -> writeNumericColumn(
                field,
                type,
                () -> ColumnarNumericBinaryDocValues.decodePayloads(valuesProducer.getBinary(field))
            );
            case STRING -> writeStringColumn(
                field,
                type,
                () -> ColumnarStringBinaryDocValues.singleValues(valuesProducer.getBinary(field))
            );
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
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        switch (type) {
            case LONG, DOUBLE -> writeNumericColumn(field, type, () -> numericMergeCursor(field, mergeState));
            case STRING -> {
                Vocabulary.Terms known = unionOfDictionaries(field, mergeState);
                if (known == null) {
                    // No union to take, but the segments may have recorded what they surveyed.
                    known = combinedSummaries(field, mergeState);
                }
                final Vocabulary.Terms vocabulary = known;
                writeStringColumn(field, type, () -> stringMergeCursor(field, mergeState, vocabulary), vocabulary);
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
            // Read decoded longs directly for our own columns; fall back to the payload for anything else.
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

        ColumnMergeSub(MergeState.DocMap docMap, T values) {
            super(docMap);
            this.values = values;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /**
     * The string counterpart of {@link #numericMergeCursor}: reads each source segment's values off disk via
     * {@link ColumnarStringBinaryDocValues#directValues}, in merged doc order. A fresh cursor is built per
     * pass — the count, the iterator, then the values.
     */
    /**
     * The union of the segments' dictionaries, or null when it cannot stand for the merged column: a
     * segment without a dictionary, or one that let values escape, holds values the union would not name.
     * It is bounded by the same policy as a surveyed vocabulary, and abandoned once it exceeds it.
     */
    private Vocabulary.Terms unionOfDictionaries(FieldInfo field, MergeState mergeState) throws IOException {
        if (dictionaryPolicy.enabled() == false) {
            return null;
        }
        final TreeSet<BytesRef> union = new TreeSet<>();
        long unionBytes = 0;
        long columnBytes = 0;
        final BytesRef term = new BytesRef();
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
            if (reader.numValues() == 0) {
                // A segment the field never appeared in has nothing to contribute and nothing to disagree
                // with; it must not decide the shape of the merged column.
                continue;
            }
            if (reader.hasDictionary() == false || reader.escapeCount() > 0) {
                return null;
            }
            for (int ordinal = 0; ordinal < reader.dictionarySize(); ordinal++) {
                reader.termAt(ordinal, term);
                if (union.add(BytesRef.deepCopyOf(term))) {
                    unionBytes += term.length;
                    if (unionBytes > dictionaryPolicy.maxBytes()) {
                        return null;
                    }
                }
            }
            columnBytes += reader.valueBytes();
        }
        if (union.isEmpty()) {
            return null;
        }
        // How often each term is used is not recorded in a dictionary column, so the union carries no
        // counts. It does not need them: it names every value, and a merge of these segments always
        // prefers it to a survey.
        return Vocabulary.known(new ArrayList<>(union), columnBytes, 1.0, null);
    }

    /**
     * A vocabulary combined from the segments' summaries. Counts are summed and trimmed to the policy's
     * bound as a survey trims, so a term the merged column holds often enough survives; the coverage is an
     * under-estimate because each summed count was.
     */
    private Vocabulary.Terms combinedSummaries(FieldInfo field, MergeState mergeState) throws IOException {
        if (dictionaryPolicy.enabled() == false) {
            return null;
        }
        final Map<BytesRef, Long> combined = new HashMap<>();
        // Bounded as it goes, so the map grows with what one segment's dictionary can describe rather than
        // with the number of segments merged. A term the merged column holds often enough is in every
        // summary that saw it, so it outlives the terms trimmed here.
        final long combinedBound = 4L * dictionaryPolicy.maxBytes();
        long combinedBytes = 0;
        long numValues = 0;
        long columnBytes = 0;
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
            if (reader.numValues() == 0) {
                continue;
            }
            if (reader.hasSummary() == false) {
                return null;
            }
            final List<BytesRef> terms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            reader.readSummary(terms, counts);
            for (int t = 0; t < terms.size(); t++) {
                if (combined.merge(terms.get(t), counts.get(t), Long::sum).equals(counts.get(t))) {
                    combinedBytes += terms.get(t).length;
                }
            }
            numValues += reader.summaryValues();
            columnBytes += reader.valueBytes();
            if (combinedBytes > combinedBound) {
                combinedBytes = trimToBound(combined, combinedBound);
            }
        }
        if (combined.isEmpty() || numValues == 0) {
            return null;
        }
        // Keep the terms seen most; the rest escape. Ties break by term, so the same inputs always yield
        // the same column.
        final List<Map.Entry<BytesRef, Long>> ranked = new ArrayList<>(combined.entrySet());
        ranked.sort(Map.Entry.<BytesRef, Long>comparingByValue().reversed().thenComparing(Map.Entry::getKey));
        final TreeSet<BytesRef> kept = new TreeSet<>();
        long bytes = 0;
        long covered = 0;
        final long budget = dictionaryPolicy.budgetFor(columnBytes);
        for (Map.Entry<BytesRef, Long> entry : ranked) {
            // As at flush: a term the merged column holds once does not repay a dictionary entry.
            if (entry.getValue() <= 1) {
                break;
            }
            if (bytes + entry.getKey().length > budget) {
                break;
            }
            kept.add(entry.getKey());
            bytes += entry.getKey().length;
            covered += entry.getValue();
        }
        if (kept.isEmpty()) {
            return null;
        }
        // Worth a dictionary or not is left to the gate a surveyed vocabulary passes; either way the merged
        // column keeps a summary.
        final List<BytesRef> sorted = new ArrayList<>(kept);
        final long[] countsPerTerm = new long[sorted.size()];
        for (int t = 0; t < sorted.size(); t++) {
            countsPerTerm[t] = combined.get(sorted.get(t));
        }
        return Vocabulary.known(sorted, columnBytes, (double) covered / numValues, countsPerTerm);
    }

    /** Drops the least frequent terms until the terms held fit {@code bound}, and returns what they weigh. */
    private static long trimToBound(Map<BytesRef, Long> combined, long bound) {
        final List<Map.Entry<BytesRef, Long>> ranked = new ArrayList<>(combined.entrySet());
        ranked.sort(Map.Entry.<BytesRef, Long>comparingByValue().reversed().thenComparing(Map.Entry::getKey));
        long bytes = 0;
        int kept = 0;
        while (kept < ranked.size() && bytes + ranked.get(kept).getKey().length <= bound) {
            bytes += ranked.get(kept).getKey().length;
            kept++;
        }
        for (int i = kept; i < ranked.size(); i++) {
            combined.remove(ranked.get(i).getKey());
        }
        return bytes;
    }

    /**
     * What each of a segment's dictionary ordinals becomes in the merged column, or null when the segment
     * has no dictionary the merged vocabulary was built from.
     */
    private static int[] ordinalMap(BinaryDocValues values, Vocabulary.Terms vocabulary) throws IOException {
        if (vocabulary == null || (values instanceof ColumnarStringBinaryDocValues) == false) {
            return null;
        }
        final StringColumnReader reader = ((ColumnarStringBinaryDocValues) values).reader();
        if (reader.hasDictionary() == false) {
            return null;
        }
        final int[] map = new int[reader.dictionarySize()];
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < map.length; ordinal++) {
            reader.termAt(ordinal, term);
            final int id = vocabulary.terms().find(term);
            if (id < 0 || vocabulary.ordinalOfId()[id] == Vocabulary.DROPPED) {
                // The merged vocabulary was built from these dictionaries, so every term should be in it.
                return null;
            }
            map[ordinal] = vocabulary.ordinalOfId()[id];
        }
        return map;
    }

    private static StringColumnValues stringMergeCursor(FieldInfo field, MergeState mergeState, Vocabulary.Terms vocabulary)
        throws IOException {
        List<ColumnMergeSub<StringColumnValues>> subs = new ArrayList<>();
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
            // Read decoded values directly for our own columns; fall back to the payload for anything else.
            StringColumnValues values = binary instanceof ColumnarStringBinaryDocValues columnar
                ? columnar.directValues(ordinalMap(binary, vocabulary))
                : ColumnarStringBinaryDocValues.singleValues(binary);
            cost += values.cost();
            subs.add(new ColumnMergeSub<>(mergeState.docMaps[i], values));
        }

        DocIDMerger<ColumnMergeSub<StringColumnValues>> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new StringColumnValues() {
            private ColumnMergeSub<StringColumnValues> current;
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
            public int nextOrdinal() throws IOException {
                return current.values.nextOrdinal();
            }

            @Override
            public int valueCount() {
                return current.values.valueCount();
            }

            @Override
            public BytesRef nextValue() throws IOException {
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
            cursors,
            pipeline,
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, type.id(), metadata));
    }

    /**
     * Counts the column in one pass, then streams the values block by block from fresh cursors — never
     * buffering the whole field on-heap.
     */
    private void writeStringColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<StringColumnValues> cursors) throws IOException {
        writeStringColumn(field, type, cursors, null);
    }

    private void writeStringColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<StringColumnValues> cursors, Vocabulary.Terms known)
        throws IOException {
        int numDocsWithField = 0;
        long numValues = 0;
        StringColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            // One value per document: that is what this surface carries, and what lets the reader take a
            // document's rank as its value's address rather than keeping one for every document.
            assert counter.valueCount() == 1 : "document [" + doc + "] of field [" + field.name + "] has " + counter.valueCount();
            numValues++;
        }

        StringColumnMetadata metadata = StringColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            cursors,
            ValueStream.VALUES_PER_BLOCK,
            ChunkCodec.ZSTD,
            TARGET_CHUNK_BYTES,
            dictionaryPolicy,
            known,
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, type.id(), metadata));
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
                + " doc values; store the field as a binary doc-values field carrying the '"
                + ColumNARDocValuesFormat.TYPE_ATTRIBUTE
                + "' attribute"
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
                meta.writeByte(entry.fieldTypeId());
                entry.metadata().writeTo(meta);
            }
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
            CodecUtil.writeFooter(data);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
        }
    }
}
