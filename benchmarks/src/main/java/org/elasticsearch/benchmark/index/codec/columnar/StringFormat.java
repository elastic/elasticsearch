/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** The storage shapes a keyword column can take, and how each one answers a grouping pass. */
public enum StringFormat {

    /** Stock Lucene, as {@code SORTED}: segment-wide ordinals resolved through a term dictionary. */
    LUCENE_SORTED,
    /** ES819 as {@code SORTED}. */
    ES819_SORTED,
    /** ES819 as {@code BINARY}: the values compressed with no ordinals, the closest stock analogue to plain. */
    ES819_BINARY,
    /** ES95 as {@code SORTED}, whose ordinals go through its own numeric codec rather than Lucene's. */
    ES95_SORTED,
    /** ColumNAR with no dictionary: values in written order. */
    COLUMNAR_PLAIN,
    /** ColumNAR with a dictionary, an ordinal per document, and an exception stream. */
    COLUMNAR_DICTIONARY,
    /** ColumNAR as it ships: the shape is whichever the values earn under the default policy. */
    COLUMNAR;

    static final String FIELD = "value";
    private static final String COLUMNAR_FILE = "string.cnd";

    boolean isColumnar() {
        return this == COLUMNAR_PLAIN || this == COLUMNAR_DICTIONARY || this == COLUMNAR;
    }

    /** Writes the column and returns the bytes it occupies. */
    long write(Directory directory, BytesRef[] values) throws IOException {
        if (isColumnar()) {
            return writeColumnar(directory, values);
        }
        final DocValuesFormat dv = switch (this) {
            case LUCENE_SORTED -> new Lucene90DocValuesFormat();
            case ES819_SORTED -> new ES819TSDBDocValuesFormat();
            case ES819_BINARY -> new ES819TSDBDocValuesFormat(BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1);
            case ES95_SORTED -> ES95TSDBDocValuesFormatFactory.create(false, false, false, null);
            default -> throw new AssertionError(this);
        };
        final Codec codec = new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dv;
            }
        };
        // Without this a segment small enough is packed into a compound file, and the scan below finds no
        // doc-values files at all — a column that fits in one reads as though it cost nothing.
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
        // Merging adjacent segments only, so a column written in term order is still in term order once the
        // segments are one. The tiered policy merges whichever segments it likes and leaves the documents in
        // an order the values no longer follow, which is not what an index sorted by this field produces.
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (BytesRef value : values) {
                final Document doc = new Document();
                if (this == ES819_BINARY) {
                    doc.add(new BinaryDocValuesField(FIELD, value));
                } else {
                    doc.add(new SortedDocValuesField(FIELD, value));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        long bytes = 0;
        for (String file : directory.listAll()) {
            if (file.endsWith(".dvd") || file.endsWith(".dvm")) {
                bytes += directory.fileLength(file);
            }
        }
        return bytes;
    }

    /**
     * Writes the values as one segment per {@code segmentSize} documents, optionally merging them into one
     * afterwards, and returns the bytes the doc values occupy.
     */
    long writeSegments(Directory directory, BytesRef[] values, int segmentSize, boolean merge) throws IOException {
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codecFor()).setUseCompoundFile(false);
        // Without a merge asked for, none is allowed: the log policy merges ten like-sized segments on its
        // own, so writing ten of them would quietly merge them and leave both the write being measured and
        // any merge measured afterwards describing something else entirely.
        iwc.setMergePolicy(merge ? new LogByteSizeMergePolicy() : NoMergePolicy.INSTANCE);
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (int i = 0; i < values.length; i++) {
                writer.addDocument(document(values[i]));
                if (i % segmentSize == segmentSize - 1) {
                    writer.flush();
                }
            }
            if (merge) {
                writer.forceMerge(1);
            }
        }
        long bytes = 0;
        for (String file : directory.listAll()) {
            if (file.endsWith(".dvd") || file.endsWith(".dvm") || file.endsWith(".cnd") || file.endsWith(".cnm")) {
                bytes += directory.fileLength(file);
            }
        }
        return bytes;
    }

    /**
     * Merges segments already on disk into one, and returns the bytes the doc values occupy afterwards.
     * Kept apart from writing them because the two answer different questions: a format that writes quickly
     * by leaving work for the merge has saved nothing, and a merge is paid again every time one happens.
     */
    long mergeSegments(Directory directory) throws IOException {
        assert segmentCount(directory) > 1 : "nothing to merge: " + segmentCount(directory) + " segment(s)";
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codecFor()).setUseCompoundFile(false);
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            writer.forceMerge(1);
        }
        long bytes = 0;
        for (String file : directory.listAll()) {
            if (file.endsWith(".dvd") || file.endsWith(".dvm") || file.endsWith(".cnd") || file.endsWith(".cnm")) {
                bytes += directory.fileLength(file);
            }
        }
        return bytes;
    }

    /** Segments in the directory, so a merge benchmark can refuse to measure a merge that has nothing to do. */
    static int segmentCount(Directory directory) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            return reader.leaves().size();
        }
    }

    private Document document(BytesRef value) {
        final Document doc = new Document();
        if (isColumnar()) {
            doc.add(new Field(FIELD, value, columnarFieldType()));
        } else if (this == ES819_BINARY) {
            doc.add(new BinaryDocValuesField(FIELD, value));
        } else {
            doc.add(new SortedDocValuesField(FIELD, value));
        }
        return doc;
    }

    private static FieldType columnarFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute("columnar.type", "STRING");
        type.freeze();
        return type;
    }

    /**
     * {@link #COLUMNAR} leaves the shape to the data, which is what ships. The other two force one shape
     * each, including on columns that would never earn it — which is how a shape is measured where it does
     * not belong, not a configuration to read as the format's behaviour.
     */
    private DictionaryPolicy dictionaryPolicy() {
        return switch (this) {
            case COLUMNAR -> ColumNARDocValuesFormat.DEFAULT_DICTIONARY_POLICY;
            // Enough for every shape here to name all of its values: a budget that stops short leaves a
            // dictionary most values escape, which measures the shortfall rather than the layout.
            case COLUMNAR_DICTIONARY -> new DictionaryPolicy(4 << 20, 0.0, Double.MAX_VALUE);
            case COLUMNAR_PLAIN -> DictionaryPolicy.NONE;
            default -> throw new IllegalStateException("not a columnar format: " + this);
        };
    }

    private Codec codecFor() {
        final DocValuesFormat dv = switch (this) {
            case LUCENE_SORTED -> new Lucene90DocValuesFormat();
            case ES819_SORTED -> new ES819TSDBDocValuesFormat();
            case ES819_BINARY -> new ES819TSDBDocValuesFormat(BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1);
            case ES95_SORTED -> ES95TSDBDocValuesFormatFactory.create(false, false, false, null);
            case COLUMNAR_PLAIN, COLUMNAR_DICTIONARY, COLUMNAR -> new ColumNARDocValuesFormat(
                (fieldName, fieldType) -> org.elasticsearch.columnar.numeric.NumericPipeline::defaultPipeline,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                dictionaryPolicy()
            );
        };
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dv;
            }
        };
    }

    private long writeColumnar(Directory directory, BytesRef[] values) throws IOException {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute("columnar.type", "STRING");
        type.freeze();
        final DocValuesFormat dv = new ColumNARDocValuesFormat(
            (fieldName, fieldType) -> org.elasticsearch.columnar.numeric.NumericPipeline::defaultPipeline,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            dictionaryPolicy()
        );
        final Codec codec = new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dv;
            }
        };
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
        // Merging adjacent segments only, so a column written in term order is still in term order once the
        // segments are one. The tiered policy merges whichever segments it likes and leaves the documents in
        // an order the values no longer follow, which is not what an index sorted by this field produces.
        iwc.setMergePolicy(new LogByteSizeMergePolicy());
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (BytesRef value : values) {
                final Document doc = new Document();
                doc.add(new Field(FIELD, value, type));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        long bytes = 0;
        for (String file : directory.listAll()) {
            if (file.endsWith(".cnd") || file.endsWith(".cnm")) {
                bytes += directory.fileLength(file);
            }
        }
        return bytes;
    }

    Column open(Directory directory, int docCount, int pageSize) throws IOException {
        return isColumnar() ? new ColumnarColumn(directory, docCount, pageSize) : new StockColumn(this, directory, pageSize);
    }

    /** A column opened for reading, which can run a pass over every document for each consumer shape. */
    interface Column extends Closeable {
        /** The ESQL shape: a page's dictionary is rebuilt and rehashed per page. */
        long group() throws IOException;

        /**
         * The aggregation shape: ordinals are stable for the whole segment, so a counter array replaces the
         * hash entirely and only the values that have no ordinal fall back to one. This is what
         * {@code GlobalOrdinalsStringTermsAggregator} does within a segment, and what
         * {@code MapStringTermsAggregator} cannot do.
         */
        long aggregate() throws IOException;

        /**
         * Every value read and its bytes touched, with no hashing. Isolates what the format costs from what
         * a consumer costs: the grouping shapes are dominated by hashing at high cardinality, which every
         * format pays alike.
         */
        long scan() throws IOException;

        /**
         * Every value read one document at a time through the doc values API, the shape of a fetch or of an
         * aggregation that works a document at a time. {@link #scan()} lets a format read however it reads
         * best; this asks all of them for one value at a time, which is all some consumers can ask for.
         */
        long readPerDocument() throws IOException;

        /** Documents whose value is {@code term}, the shape of {@code WHERE field == "..."}. */
        long matchTerm(BytesRef term) throws IOException;

        /** Documents whose value starts with {@code prefix}, the shape of {@code WHERE field LIKE "x*"}. */
        long matchPrefix(BytesRef prefix) throws IOException;

        /**
         * Documents holding {@code term} somewhere inside their value, the shape of
         * {@code WHERE field LIKE "*x*"}. Order is no help, so every format has to look at what the values
         * say; what differs is how many of them it has to look at.
         */
        long matchContains(BytesRef term) throws IOException;

        /**
         * The term as a query run through an {@link org.apache.lucene.search.IndexSearcher}. This is the
         * path a filter actually takes: a scorer collects a window at a time, which is what a two-phase
         * iterator exists to make cheap and what asking it document by document throws away.
         */
        long queryTerm(BytesRef term) throws IOException;

        /** How the column stored its values, for a run to report what it measured. */
        String shape() throws IOException;

        /** The prefix as a query, the shape of {@code LIKE "x*"}. */
        long queryPrefix(BytesRef prefix) throws IOException;
    }

    /**
     * ColumNAR: the reader picks the form per page, and the sink hashes whichever it gets — the distinct
     * values of the page when it gets ordinals, every row when it gets values.
     */
    private static final class ColumnarColumn implements Column {
        private final DirectoryReader directoryReader;
        private final IndexSearcher searcher;
        private final LeafReader leaf;
        private final StringColumnReader reader;
        private final int[] docs;
        private final int pageSize;

        ColumnarColumn(Directory directory, int docCount, int pageSize) throws IOException {
            this.directoryReader = DirectoryReader.open(directory);
            this.searcher = new IndexSearcher(directoryReader);
            this.searcher.setQueryCache(null);
            this.leaf = directoryReader.leaves().get(0).reader();
            this.reader = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
            this.pageSize = pageSize;
            this.docs = new int[docCount];
            for (int i = 0; i < docCount; i++) {
                docs[i] = i;
            }
        }

        @Override
        public long matchTerm(BytesRef term) throws IOException {
            return count(reader.matchTerm(term));
        }

        @Override
        public long matchPrefix(BytesRef prefix) throws IOException {
            return count(reader.matchPrefix(prefix));
        }

        @Override
        public long matchContains(BytesRef term) throws IOException {
            return count(reader.matchContains(term));
        }

        @Override
        public long queryTerm(BytesRef term) throws IOException {
            return bulkCount(searcher, directoryReader.leaves().get(0), ColumnarStringTermQuery.term(FIELD, term));
        }

        @Override
        public String shape() {
            final String shape = reader.hasDictionary() ? "dictionary(" + reader.dictionarySize() + ")" : "plain";
            return reader.valuesSorted() ? shape + "+sorted" : shape;
        }

        @Override
        public long queryPrefix(BytesRef prefix) throws IOException {
            return bulkCount(searcher, directoryReader.leaves().get(0), ColumnarStringTermQuery.prefix(FIELD, prefix));
        }

        private static long count(DocIdSetIterator matches) throws IOException {
            long found = 0;
            for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
                found++;
            }
            return found;
        }

        @Override
        public long group() throws IOException {
            try (GroupingSink sink = new GroupingSink()) {
                for (int start = 0; start < docs.length; start += pageSize) {
                    reader.readBlock(docs, start, Math.min(pageSize, docs.length - start), sink);
                }
                return sink.checksum;
            }
        }

        @Override
        public long scan() throws IOException {
            final LengthSink sink = new LengthSink();
            for (int start = 0; start < docs.length; start += pageSize) {
                reader.readBlock(docs, start, Math.min(pageSize, docs.length - start), sink);
            }
            return sink.checksum;
        }

        @Override
        public long readPerDocument() throws IOException {
            long checksum = 0;
            final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                checksum += values.binaryValue().length;
            }
            return checksum;
        }

        @Override
        public long aggregate() throws IOException {
            final int dictionarySize = reader.dictionarySize();
            if (dictionarySize == 0) {
                // No ordinals stable for the column, so this is map-mode aggregation: every distinct value
                // is hashed. It still reads a page at a time, because a page hands back one entry per run
                // of equal values and hashing those is what a map-mode aggregator would do here.
                final MapModeSink sink = new MapModeSink();
                for (int start = 0; start < docs.length; start += pageSize) {
                    reader.readBlock(docs, start, Math.min(pageSize, docs.length - start), sink);
                }
                return sink.checksum;
            }
            // A non-zero dictionary size is what says this column has one.
            final DictionaryStringColumnReader dictionary = (DictionaryStringColumnReader) reader;
            final long[] counts = new long[dictionarySize];
            final int[] ordinals = new int[pageSize];
            // An escaped value is reached by its address, which a document's rank gives.
            final int[] ranks = new int[pageSize];
            final BytesRefHash escaped = new BytesRefHash();
            final BytesRef scratch = new BytesRef();
            long checksum = 0;
            for (int start = 0; start < docs.length; start += pageSize) {
                final int count = Math.min(pageSize, docs.length - start);
                reader.readOrdinals(docs, start, count, ordinals);
                reader.ranks(docs, start, count, ranks);
                for (int i = 0; i < count; i++) {
                    final int ordinal = ordinals[i];
                    if (ordinal < dictionarySize) {
                        counts[ordinal]++;
                    } else {
                        checksum += StringFormat.group(escaped, dictionary.resolveEscape(reader.firstValueAddress(ranks[i]), scratch));
                    }
                }
            }
            for (long value : counts) {
                checksum += value;
            }
            return checksum;
        }

        @Override
        public void close() throws IOException {
            directoryReader.close();
        }
    }

    /** A stock format, grouped the way ESQL does: a page's dictionary is resolved and hashed per page. */
    private static final class StockColumn implements Column {
        private final StringFormat format;
        private final DirectoryReader reader;
        private final IndexSearcher searcher;
        private final LeafReader leaf;
        private final int pageSize;

        StockColumn(StringFormat format, Directory directory, int pageSize) throws IOException {
            this.format = format;
            this.reader = DirectoryReader.open(directory);
            this.searcher = new IndexSearcher(this.reader);
            this.searcher.setQueryCache(null);
            this.leaf = reader.leaves().get(0).reader();
            this.pageSize = pageSize;
        }

        @Override
        public String shape() {
            return format == ES819_BINARY ? "binary" : "sorted";
        }

        @Override
        public long group() throws IOException {
            return format == ES819_BINARY ? groupBinary() : groupSorted();
        }

        /** No ordinals to exploit, so every row is hashed. */
        private long groupBinary() throws IOException {
            final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
            final BytesRefHash groups = new BytesRefHash();
            long checksum = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                checksum += StringFormat.group(groups, values.binaryValue());
            }
            return checksum;
        }

        /**
         * Segment ordinals gathered per page, the distinct ones resolved and hashed, then the rows mapped —
         * which is what {@code SingletonOrdinalsBuilder#buildOrdinal} plus {@code BlockHash} amounts to. The
         * resolution is dropped at the page boundary because that builder rebuilds its dictionary per page.
         */
        private long groupSorted() throws IOException {
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final int[] pageOrds = new int[pageSize];
            final int[] touched = new int[pageSize];
            final int[] groupOf = new int[values.getValueCount()];
            Arrays.fill(groupOf, -1);
            final BytesRefHash groups = new BytesRefHash();
            long checksum = 0;
            int inPage = 0;
            int touchedCount = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                final int ord = values.ordValue();
                pageOrds[inPage++] = ord;
                if (groupOf[ord] == -1) {
                    groupOf[ord] = -2;
                    touched[touchedCount++] = ord;
                }
                if (inPage == pageSize) {
                    checksum += flush(values, groups, pageOrds, inPage, touched, touchedCount, groupOf);
                    inPage = 0;
                    touchedCount = 0;
                }
            }
            if (inPage > 0) {
                checksum += flush(values, groups, pageOrds, inPage, touched, touchedCount, groupOf);
            }
            return checksum;
        }

        private static long flush(
            SortedDocValues values,
            BytesRefHash groups,
            int[] pageOrds,
            int inPage,
            int[] touched,
            int touchedCount,
            int[] groupOf
        ) throws IOException {
            for (int i = 0; i < touchedCount; i++) {
                groupOf[touched[i]] = StringFormat.group(groups, values.lookupOrd(touched[i]));
            }
            long checksum = 0;
            for (int i = 0; i < inPage; i++) {
                checksum += groupOf[pageOrds[i]];
            }
            for (int i = 0; i < touchedCount; i++) {
                groupOf[touched[i]] = -1;
            }
            return checksum;
        }

        /**
         * A term over segment ordinals: the dictionary is searched once and then every document's ordinal is
         * compared to the one it found, which is what a doc-values term query does. Without ordinals there is
         * nothing to compare but the values themselves.
         */
        @Override
        public long matchTerm(BytesRef term) throws IOException {
            if (format == ES819_BINARY) {
                final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
                long found = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    if (values.binaryValue().bytesEquals(term)) {
                        found++;
                    }
                }
                return found;
            }
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final int ordinal = values.lookupTerm(term);
            if (ordinal < 0) {
                return 0;
            }
            long found = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (values.ordValue() == ordinal) {
                    found++;
                }
            }
            return found;
        }

        /**
         * Lucene's own doc-values term query, so both sides run through a scorer. A keyword field would
         * normally be filtered through the inverted index instead; this compares what the doc-values
         * formats themselves can do, which is the only thing a doc-values format can be asked to do.
         */
        @Override
        public long queryTerm(BytesRef term) throws IOException {
            if (format == ES819_BINARY) {
                // Binary doc values carry no queryable form, so the comparison is the scan itself.
                return matchTerm(term);
            }
            // The exact query rather than a set of one: a set builds a bitset of ordinals to test against,
            // which is work a single term does not need and would not be a fair thing to charge it for.
            return bulkCount(searcher, reader.leaves().get(0), SortedDocValuesField.newSlowExactQuery(FIELD, term));
        }

        @Override
        public long queryPrefix(BytesRef prefix) throws IOException {
            if (format == ES819_BINARY) {
                return matchPrefix(prefix);
            }
            // A prefix is a run of ordinals; the terms it covers are collected once and asked for together.
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final int start = insertionPoint(values.lookupTerm(prefix));
            final List<BytesRef> terms = new ArrayList<>();
            for (int ordinal = start; ordinal < values.getValueCount(); ordinal++) {
                final BytesRef term = values.lookupOrd(ordinal);
                if (startsWith(term, prefix) == false) {
                    break;
                }
                terms.add(BytesRef.deepCopyOf(term));
            }
            return terms.isEmpty() ? 0 : bulkCount(searcher, reader.leaves().get(0), SortedDocValuesField.newSlowSetQuery(FIELD, terms));
        }

        @Override
        public long matchPrefix(BytesRef prefix) throws IOException {
            if (format == ES819_BINARY) {
                final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
                long found = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    if (startsWith(values.binaryValue(), prefix)) {
                        found++;
                    }
                }
                return found;
            }
            // Sorted ordinals, so the prefix is a run of them; its bounds are found once and then every
            // document's ordinal is tested against them.
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final int start = insertionPoint(values.lookupTerm(prefix));
            int end = start;
            while (end < values.getValueCount() && startsWith(values.lookupOrd(end), prefix)) {
                end++;
            }
            if (start == end) {
                return 0;
            }
            long found = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                final int ordinal = values.ordValue();
                if (ordinal >= start && ordinal < end) {
                    found++;
                }
            }
            return found;
        }

        /**
         * The same vectorized search Elasticsearch uses for {@code LIKE "*x*"} on a binary field, so what is
         * compared is how many values each format has to search rather than how each searches one. A format
         * with a term dictionary searches each term once and tests ordinals after, which is the same saving
         * the columnar dictionary makes.
         */
        @Override
        public long matchContains(BytesRef term) throws IOException {
            if (format == ES819_BINARY) {
                final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
                long found = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    final BytesRef value = values.binaryValue();
                    if (ESVectorUtil.contains(value.bytes, value.offset, value.length, term.bytes, term.offset, term.length)) {
                        found++;
                    }
                }
                return found;
            }
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final int size = values.getValueCount();
            final FixedBitSet matching = new FixedBitSet(Math.max(1, size));
            for (int ordinal = 0; ordinal < size; ordinal++) {
                final BytesRef candidate = values.lookupOrd(ordinal);
                if (ESVectorUtil.contains(candidate.bytes, candidate.offset, candidate.length, term.bytes, term.offset, term.length)) {
                    matching.set(ordinal);
                }
            }
            if (matching.cardinality() == 0) {
                return 0;
            }
            long found = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (matching.get(values.ordValue())) {
                    found++;
                }
            }
            return found;
        }

        private static int insertionPoint(int lookup) {
            return lookup >= 0 ? lookup : -1 - lookup;
        }

        private static boolean startsWith(BytesRef value, BytesRef prefix) {
            if (value.length < prefix.length) {
                return false;
            }
            return Arrays.equals(
                value.bytes,
                value.offset,
                value.offset + prefix.length,
                prefix.bytes,
                prefix.offset,
                prefix.offset + prefix.length
            );
        }

        @Override
        public long scan() throws IOException {
            long checksum = 0;
            if (format == ES819_BINARY) {
                final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    checksum += values.binaryValue().length;
                }
                return checksum;
            }
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                checksum += values.lookupOrd(values.ordValue()).length;
            }
            return checksum;
        }

        @Override
        public long readPerDocument() throws IOException {
            // The stock formats have no bulk read here: scan already asks for one value at a time.
            return scan();
        }

        @Override
        public long aggregate() throws IOException {
            if (format == ES819_BINARY) {
                return groupBinary();
            }
            // Segment ordinals are stable across pages, so the counter array is indexed directly and no
            // value is ever hashed or resolved.
            final SortedDocValues values = leaf.getSortedDocValues(FIELD);
            final long[] counts = new long[values.getValueCount()];
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                counts[values.ordValue()]++;
            }
            long checksum = 0;
            for (long count : counts) {
                checksum += count;
            }
            return checksum;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /** Touches every value's bytes and nothing else. */
    /** Hashes a page's distinct values, once each, and counts how many documents each covers. */
    private static final class MapModeSink implements StringBlockSink {

        private final BytesRefHash groups = new BytesRefHash();
        long checksum;

        @Override
        public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
            // One hash a distinct value rather than one a document, which is the whole point of the page
            // coming back as ordinals.
            for (int i = 0; i < dictionarySize; i++) {
                checksum += StringFormat.group(groups, dictionary[i]);
            }
            for (int i = 0; i < count; i++) {
                checksum += ordinals[i];
            }
        }

        @Override
        public void appendValues(BytesRef[] values, int count) {
            for (int i = 0; i < count; i++) {
                checksum += StringFormat.group(groups, values[i]);
            }
        }
    }

    private static final class LengthSink implements StringBlockSink {
        long checksum;

        @Override
        public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
            for (int i = 0; i < count; i++) {
                checksum += dictionary[ordinals[i]].length;
            }
        }

        @Override
        public void appendValues(BytesRef[] values, int count) {
            for (int i = 0; i < count; i++) {
                checksum += values[i].length;
            }
        }
    }

    /** Models a block hash: one hash per distinct value in a page, one array lookup per row. */
    private static final class GroupingSink implements StringBlockSink, Closeable {
        private final BytesRefHash groups = new BytesRefHash();
        private int[] groupOf = new int[0];
        long checksum;

        @Override
        public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
            if (groupOf.length < dictionarySize) {
                groupOf = new int[dictionarySize];
            }
            for (int d = 0; d < dictionarySize; d++) {
                groupOf[d] = group(groups, dictionary[d]);
            }
            for (int i = 0; i < count; i++) {
                checksum += groupOf[ordinals[i]];
            }
        }

        @Override
        public void appendValues(BytesRef[] values, int count) {
            for (int i = 0; i < count; i++) {
                checksum += group(groups, values[i]);
            }
        }

        @Override
        public void close() {}
    }

    /**
     * Runs a query the way a search runs it: through the weight's bulk scorer, which is where a constant
     * score query collects a window at a time and an iterator's {@code intoBitSet} is reached.
     * {@code IndexSearcher#count} does not take that path.
     */
    private static long bulkCount(IndexSearcher searcher, LeafReaderContext context, Query query) throws IOException {
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final BulkScorer scorer = weight.bulkScorer(context);
        if (scorer == null) {
            return 0;
        }
        final long[] found = { 0 };
        scorer.score(new LeafCollector() {
            @Override
            public void setScorer(Scorable scorable) {}

            @Override
            public void collect(int doc) {
                found[0]++;
            }
        }, context.reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        return found[0];
    }

    private static int group(BytesRefHash groups, BytesRef value) {
        final int id = groups.add(value);
        return id < 0 ? -1 - id : id;
    }

}
