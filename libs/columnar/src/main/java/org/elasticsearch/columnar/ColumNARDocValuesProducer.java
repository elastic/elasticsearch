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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.MetricRecorder;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Timer;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the columns written by {@link ColumNARDocValuesConsumer}, served at the {@code BINARY} surface
 * ({@link #getBinary}). Fields are framed generically ({@code fieldNumber}, {@link ColumnarFieldType} id,
 * string framing, metadata), so a new column type slots in by extending the read dispatch here.
 */
final class ColumNARDocValuesProducer extends DocValuesProducer {

    static final CounterMetric READERS_OPENED = CounterMetric.of(
        "ColumNAR",
        "readers",
        "opened",
        Metric.Unit.COUNT,
        "Columns opened for reading, once for each request for a column. Readers are not reused across requests. It equals "
            + "readers.plain plus readers.dictionary plus readers.numeric."
    );

    static final CounterMetric PLAIN_READERS_OPENED = CounterMetric.of(
        "ColumNAR",
        "readers",
        "plain",
        Metric.Unit.COUNT,
        "String columns opened for reading that store their values one after another, without a dictionary."
    );

    static final CounterMetric DICTIONARY_READERS_OPENED = CounterMetric.of(
        "ColumNAR",
        "readers",
        "dictionary",
        Metric.Unit.COUNT,
        "String columns opened for reading that name their values with ordinals into a dictionary."
    );

    static final CounterMetric NUMERIC_READERS_OPENED = CounterMetric.of(
        "ColumNAR",
        "readers",
        "numeric",
        Metric.Unit.COUNT,
        "Numeric columns opened for reading."
    );

    static final TimerMetric READER_OPEN_TIME = TimerMetric.of(
        "ColumNAR",
        "readers",
        "open_time_nanos",
        TimingPolicy.EXACT,
        "Elapsed wall-clock time of requests for a column, measured exactly. It covers building the reader and nothing the reader "
            + "does afterwards. It is not CPU time and it can include waits for data."
    );

    private static final Map<String, Boolean> WITH_SKIP_INDEX = Map.of("skip_index", true);
    private static final Map<String, Boolean> WITHOUT_SKIP_INDEX = Map.of("skip_index", false);

    private final int maxDoc;
    /** The name of the segment, which identifies its readers in a profile. */
    private final String segmentName;
    private final IndexInput data;
    private final IndexInput addressing;
    private final IndexInput lengths;
    private final IndexInput navigation;
    private final IndexInput skipIndex;
    private final ColumnInputs inputs;
    private final Map<Integer, Column> columns = new HashMap<>();
    private boolean closed = false;

    /** A read-side column: its declared type, how a string column's blobs are framed, and the metadata needed to open it. */
    private record Column(ColumnarFieldType type, boolean singleValued, ColumnMetadata metadata) {}

    ColumNARDocValuesProducer(SegmentReadState state) throws IOException {
        this.maxDoc = state.segmentInfo.maxDoc();
        this.segmentName = state.segmentInfo.name;

        String metaName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ColumNARDocValuesFormat.META_EXTENSION
        );
        FormatVersion metaVersion = null;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
            Throwable priorException = null;
            try {
                metaVersion = ColumnarCodecUtil.checkHeader(
                    meta,
                    ColumNARDocValuesFormat.META_CODEC,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
                    columns.put(fieldNumber, readColumn(ColumnarFieldType.fromId(meta.readByte()), meta, metaVersion));
                }
            } catch (Throwable e) {
                priorException = e;
            } finally {
                CodecUtil.checkFooter(meta, priorException);
            }
        }

        boolean success = false;
        try {
            data = openInput(state, state.context, ColumNARDocValuesFormat.DATA_EXTENSION, ColumNARDocValuesFormat.DATA_CODEC, metaVersion);
            addressing = openInput(
                state,
                state.context,
                ColumNARDocValuesFormat.ADDRESSING_EXTENSION,
                ColumNARDocValuesFormat.ADDRESSING_CODEC,
                metaVersion
            );
            lengths = openInput(
                state,
                state.context,
                ColumNARDocValuesFormat.LENGTHS_EXTENSION,
                ColumNARDocValuesFormat.LENGTHS_CODEC,
                metaVersion
            );
            // Index-like: every read consults the navigation, and the skip index decides what to read.
            navigation = openInput(
                state,
                state.context.withHints(FileTypeHint.INDEX),
                ColumNARDocValuesFormat.NAVIGATION_EXTENSION,
                ColumNARDocValuesFormat.NAVIGATION_CODEC,
                metaVersion
            );
            skipIndex = openInput(
                state,
                state.context.withHints(FileTypeHint.INDEX),
                ColumNARDocValuesFormat.SKIP_EXTENSION,
                ColumNARDocValuesFormat.SKIP_CODEC,
                metaVersion
            );
            inputs = new ColumnInputs(data, addressing, lengths, navigation, null);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    /** Opens one of the segment's files, checking its header against the metadata's format version. */
    private static IndexInput openInput(SegmentReadState state, IOContext context, String extension, String codec, FormatVersion expected)
        throws IOException {
        final String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
        final IndexInput in = state.directory.openInput(name, context);
        boolean success = false;
        try {
            final FormatVersion version = ColumnarCodecUtil.checkHeader(in, codec, state.segmentInfo.getId(), state.segmentSuffix);
            if (expected.equals(version) == false) {
                throw new CorruptIndexException(
                    "Format versions mismatch: meta=" + expected.version() + ", " + extension + "=" + version.version(),
                    in
                );
            }
            CodecUtil.retrieveChecksum(in);
            success = true;
            return in;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }

    private Column readColumn(ColumnarFieldType type, ChecksumIndexInput meta, final FormatVersion formatVersion) throws IOException {
        return switch (type) {
            case LONG, DOUBLE -> new Column(type, false, NumericColumnMetadata.readFrom(meta, maxDoc, formatVersion));
            case STRING -> {
                final boolean singleValued = readSingleValued(meta);
                yield new Column(type, singleValued, StringColumnMetadata.readFrom(meta, maxDoc, formatVersion));
            }
        };
    }

    private static boolean readSingleValued(ChecksumIndexInput meta) throws IOException {
        final byte framing = meta.readByte();
        return switch (framing) {
            case ColumNARDocValuesFormat.COLUMNAR_PAYLOAD -> false;
            case ColumNARDocValuesFormat.PLAIN -> true;
            default -> throw new CorruptIndexException("unknown string column framing [" + framing + "]", meta);
        };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        final Column column = columnOf(field);
        return openBinary(column, Scopes.current(ColumnarFormat.NAME, segmentName, field.name, factsOf(column.metadata())));
    }

    // NOTE: Merge readers get no recorder, also when the merge runs on a thread where a query bound a scope.
    @Override
    public DocValuesProducer getMergeInstance() {
        return new DocValuesProducer() {
            @Override
            public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                return openBinary(columnOf(field), null);
            }

            @Override
            public NumericDocValues getNumeric(FieldInfo field) {
                return ColumNARDocValuesProducer.this.getNumeric(field);
            }

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                return ColumNARDocValuesProducer.this.getSortedNumeric(field);
            }

            @Override
            public SortedDocValues getSorted(FieldInfo field) {
                return ColumNARDocValuesProducer.this.getSorted(field);
            }

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo field) {
                return ColumNARDocValuesProducer.this.getSortedSet(field);
            }

            @Override
            public DocValuesSkipper getSkipper(FieldInfo field) {
                return ColumNARDocValuesProducer.this.getSkipper(field);
            }

            @Override
            public void checkIntegrity() throws IOException {
                ColumNARDocValuesProducer.this.checkIntegrity();
            }

            @Override
            public DocValuesProducer getMergeInstance() {
                return this;
            }

            @Override
            public void close() {}
        };
    }

    private Column columnOf(FieldInfo field) {
        final Column column = columns.get(field.number);
        if (column == null) {
            throw new IllegalStateException("field [" + field.name + "] is not a ColumNAR column");
        }
        return column;
    }

    private BinaryDocValues openBinary(Column column, @Nullable MetricRecorder metrics) throws IOException {
        // NOTE: Every call builds a new reader. A reader reports to the recorder it got when it opened, so a shared
        // reader would report the work of one scope to another. The open is measured here because no reader
        // component exists before it.
        final ColumnInputs scoped;
        final Counter opened;
        final Counter layout;
        final Timer openTime;
        if (metrics == null) {
            scoped = inputs;
            opened = null;
            layout = null;
            openTime = Timer.INERT;
        } else {
            opened = metrics.counter(READERS_OPENED);
            layout = layoutCounter(metrics, column.metadata());
            scoped = inputs.withMetrics(metrics);
            openTime = metrics.timer(READER_OPEN_TIME);
        }
        final BinaryDocValues reader;
        openTime.start();
        try {
            // The metadata a column carries follows from its type, so the two always agree; the sealed
            // interface is what lets a second column type arrive without another shape here.
            reader = switch (column.metadata()) {
                case NumericColumnMetadata numeric -> numericBinary(numeric, scoped);
                case StringColumnMetadata string -> stringBinary(string, column.singleValued(), scoped);
            };
        } finally {
            openTime.stop();
        }
        if (opened != null) {
            opened.add(1);
            layout.add(1);
        }
        return reader;
    }

    /** The layout counter for {@code metadata}. It also registers the other layout counters, so all three are present. */
    private static Counter layoutCounter(MetricRecorder metrics, ColumnMetadata metadata) {
        final Counter plain = metrics.counter(PLAIN_READERS_OPENED);
        final Counter dictionary = metrics.counter(DICTIONARY_READERS_OPENED);
        final Counter numeric = metrics.counter(NUMERIC_READERS_OPENED);
        return switch (metadata) {
            case NumericColumnMetadata ignored -> numeric;
            case StringColumnMetadata.Dictionary ignored -> dictionary;
            case StringColumnMetadata.Plain ignored -> plain;
        };
    }

    /**
     * The facts a profile reports about the stored column: {@code skip_index}, whether the column has the ColumNAR skip
     * index. The fact says that the index exists, not that a read used it.
     */
    private static Map<String, Boolean> factsOf(ColumnMetadata metadata) {
        return hasSkipIndex(metadata) ? WITH_SKIP_INDEX : WITHOUT_SKIP_INDEX;
    }

    /** Whether a column has ColumNAR's own skip index: a numeric column's, or the one over a dictionary column's ordinals. */
    private static boolean hasSkipIndex(ColumnMetadata metadata) {
        return switch (metadata) {
            case NumericColumnMetadata numeric -> numeric.skipper() != null;
            case StringColumnMetadata.Dictionary dictionary -> dictionary.ordinals().skipper() != null;
            case StringColumnMetadata.Plain plain -> false;
        };
    }

    private BinaryDocValues stringBinary(StringColumnMetadata metadata, boolean singleValued, ColumnInputs scoped) throws IOException {
        StringColumnReader reader = StringColumnReader.open(metadata, scoped, skipIndex);
        return new ColumnarStringBinaryDocValues(reader, reader.iterator(), singleValued);
    }

    private BinaryDocValues numericBinary(NumericColumnMetadata metadata, ColumnInputs scoped) throws IOException {
        NumericColumnReader reader = new NumericColumnReader(metadata, scoped);
        ColumnIterator iterator = reader.iterator();
        return new ColumnarNumericBinaryDocValues(reader, iterator, maxDoc, metadata.skipper(), skipIndex);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) {
        throw typedNotSupported("numeric");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw typedNotSupported("sorted-numeric");
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw typedNotSupported("sorted");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw typedNotSupported("sorted-set");
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        // Deliberately unsupported: a ColumNAR column is exposed as a BINARY field, and a BINARY field
        // cannot carry a Lucene skip index, so Lucene never calls this. Range pushdown does not rely on
        // a Lucene skipper — it uses the column's own skip index, driven by ColumnarNumericRangeQuery
        // through the binary reader.
        throw typedNotSupported("skipper");
    }

    private static UnsupportedOperationException typedNotSupported(String shape) {
        return new UnsupportedOperationException(
            "ColumNAR is a binary doc-values format and does not serve " + shape + " doc values; read the column at getBinary"
        );
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
        CodecUtil.checksumEntireFile(addressing);
        CodecUtil.checksumEntireFile(lengths);
        CodecUtil.checksumEntireFile(navigation);
        CodecUtil.checksumEntireFile(skipIndex);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.close(data, addressing, lengths, navigation, skipIndex);
    }
}
