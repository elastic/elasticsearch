/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.codec.CodecMetrics.Format;
import org.elasticsearch.index.codec.CodecMetrics.Operation;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class MetricingCodecTests extends ESTestCase {

    public void testNameRoundTripsThroughSpi() {
        Codec delegate = TestUtil.getDefaultCodec();
        MetricingCodec codec = new MetricingCodec(delegate, CodecMetrics.NOOP, randomFrom(IndexMode.values()));
        assertEquals(delegate.getName(), codec.getName());
        assertEquals(delegate.getClass(), Codec.forName(codec.getName()).getClass());
    }

    public void testCleanWriteAndMergeRecordNothing() throws IOException {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(TestUtil.getDefaultCodec(), new CodecMetrics(registry), randomFrom(IndexMode.values()));
        try (Directory dir = newDirectory()) {
            writeSegments(dir, codec, between(2, 4));
            try (IndexWriter writer = new IndexWriter(dir, config(codec, false))) {
                writer.forceMerge(1);
            }
        }
        assertThat(failures(registry), empty());
    }

    /** A failing flush is counted as a write of the failing format, whether the format fails while opening or while writing. */
    public void testWriteFailure() throws IOException {
        Format format = randomFrom(Format.values());
        Failure failure = randomFailure();
        IndexMode indexMode = randomFrom(IndexMode.values());
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec failing = new FailingCodec(format, randomFrom(FailAt.WRITER_OPEN, FailAt.WRITER), failure);
        Codec codec = new MetricingCodec(failing, new CodecMetrics(registry), indexMode);

        try (Directory dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, config(codec, true));
            expectThrows(Exception.class, () -> {
                for (int i = 0; i < between(1, 20); i++) {
                    writer.addDocument(randomDocument());
                }
                writer.commit();
            });
            IOUtils.closeWhileHandlingException(writer);
        }
        assertFailure(registry, indexMode, format, Operation.WRITE, failure);
    }

    /**
     * A failing merge is counted as a merge of the failing format. Failing to open the format's writer is indistinguishable from a
     * flush at that point, so it counts as a write even when it happens inside a merge.
     */
    public void testMergeFailure() throws IOException {
        Format format = randomFrom(Format.values());
        FailAt failAt = randomFrom(FailAt.WRITER_OPEN, FailAt.WRITER);
        Failure failure = randomFailure();
        IndexMode indexMode = randomFrom(IndexMode.values());
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(new FailingCodec(format, failAt, failure), new CodecMetrics(registry), indexMode);

        try (Directory dir = newDirectory()) {
            writeSegments(dir, TestUtil.getDefaultCodec(), between(2, 4));
            IndexWriter writer = new IndexWriter(dir, config(codec, false));
            expectThrows(Exception.class, () -> writer.forceMerge(1));
            IOUtils.closeWhileHandlingException(writer);
        }
        assertFailure(registry, indexMode, format, failAt == FailAt.WRITER_OPEN ? Operation.WRITE : Operation.MERGE, failure);
    }

    /** Segments flushed by the current writer are opened through its codec (NRT readers, merge sources), so a failing open is counted. */
    public void testOpenFailure() throws IOException {
        Format format = randomFrom(Format.values());
        Failure failure = randomFailure();
        IndexMode indexMode = randomFrom(IndexMode.values());
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(new FailingCodec(format, FailAt.READER_OPEN, failure), new CodecMetrics(registry), indexMode);

        try (Directory dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, config(codec, true));
            writer.addDocument(randomDocument());
            expectThrows(Exception.class, () -> DirectoryReader.open(writer).close());
            IOUtils.closeWhileHandlingException(writer);
        }
        assertFailure(registry, indexMode, format, Operation.OPEN, failure);
    }

    private static void writeSegments(Directory dir, Codec codec, int segments) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, config(codec, true))) {
            for (int s = 0; s < segments; s++) {
                for (int i = 0; i < between(1, 20); i++) {
                    writer.addDocument(randomDocument());
                }
                writer.commit();
            }
        }
    }

    private static IndexWriterConfig config(Codec codec, boolean noMerges) {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergeScheduler(new SerialMergeScheduler());
        return noMerges ? config.setMergePolicy(NoMergePolicy.INSTANCE) : config;
    }

    /** One value of every data-bearing format, so whichever format is set up to fail is exercised. */
    private static Document randomDocument() {
        Document doc = new Document();
        doc.add(new StringField("id", randomAlphanumericOfLength(8), Field.Store.YES));
        doc.add(new TextField("text", randomAlphaOfLengthBetween(1, 20), Field.Store.NO));
        doc.add(new NumericDocValuesField("dv", randomLong()));
        doc.add(new IntPoint("point", randomInt()));
        doc.add(new KnnFloatVectorField("vector", new float[] { randomFloat(), randomFloat() }));
        return doc;
    }

    private static List<Measurement> failures(RecordingMeterRegistry registry) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, CodecMetrics.CODEC_FAILURE_TOTAL);
    }

    private static void assertFailure(RecordingMeterRegistry registry, IndexMode indexMode, Format format, Operation op, Failure failure) {
        List<Measurement> measurements = failures(registry);
        assertThat(measurements, hasSize(1));
        assertEquals(1L, measurements.get(0).getLong());
        assertEquals(
            Map.of(
                CodecMetrics.INDEX_MODE_ATTRIBUTE,
                indexMode.getName(),
                MetricAttributes.ERROR_TYPE,
                failure.expectedErrorType,
                CodecMetrics.FORMAT_ATTRIBUTE,
                format.name().toLowerCase(Locale.ROOT),
                CodecMetrics.OPERATION_ATTRIBUTE,
                op.name().toLowerCase(Locale.ROOT)
            ),
            measurements.get(0).attributes()
        );
    }

    private record Failure(Supplier<Exception> supplier, String expectedErrorType) {
        /** Throws the failure; the return type only exists so callers can write {@code throw failure.raise()}. */
        IOException raise() throws IOException {
            Exception e = supplier.get();
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw (RuntimeException) e;
        }
    }

    private static Failure randomFailure() {
        return randomFrom(
            new Failure(() -> new CorruptIndexException("corrupt", "test"), "CorruptIndexException"),
            new Failure(() -> new IOException("io"), "IOException"),
            new Failure(() -> new IllegalStateException("state"), "IllegalStateException")
        );
    }

    private enum FailAt {
        WRITER_OPEN,
        WRITER,
        READER_OPEN
    }

    /**
     * The default codec with one format replaced by a version that fails at the chosen point. Which writer method ends up failing is
     * decided by Lucene: a flush calls the add/write methods, a merge calls merge.
     */
    private static final class FailingCodec extends FilterCodec {
        private final Format format;
        private final FailAt failAt;
        private final Failure failure;

        FailingCodec(Format format, FailAt failAt, Failure failure) {
            super(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec());
            this.format = format;
            this.failAt = failAt;
            this.failure = failure;
        }

        /** The failing format's writer: fails to open, fails on every call except the bookkeeping ones, or is the real one. */
        private <T> T writer(Class<T> type, IOSupplier<T> real) throws IOException {
            return switch (failAt) {
                case WRITER_OPEN -> throw failure.raise();
                case WRITER -> mock(type, invocation -> switch (invocation.getMethod().getName()) {
                    case "close" -> null;
                    case "ramBytesUsed" -> 0L;
                    case "getChildResources" -> List.of();
                    default -> throw failure.raise();
                });
                case READER_OPEN -> real.get();
            };
        }

        private <T> T reader(IOSupplier<T> real) throws IOException {
            if (failAt == FailAt.READER_OPEN) {
                throw failure.raise();
            }
            return real.get();
        }

        @Override
        public PostingsFormat postingsFormat() {
            PostingsFormat in = delegate.postingsFormat();
            return format != Format.POSTINGS ? in : new PostingsFormat(in.getName()) {
                @Override
                public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                    return writer(FieldsConsumer.class, () -> in.fieldsConsumer(state));
                }

                @Override
                public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                    return reader(() -> in.fieldsProducer(state));
                }
            };
        }

        @Override
        public DocValuesFormat docValuesFormat() {
            DocValuesFormat in = delegate.docValuesFormat();
            return format != Format.DOC_VALUES ? in : new DocValuesFormat(in.getName()) {
                @Override
                public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                    return writer(DocValuesConsumer.class, () -> in.fieldsConsumer(state));
                }

                @Override
                public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
                    return reader(() -> in.fieldsProducer(state));
                }
            };
        }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            StoredFieldsFormat in = delegate.storedFieldsFormat();
            return format != Format.STORED_FIELDS ? in : new StoredFieldsFormat() {
                @Override
                public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
                    throws IOException {
                    return reader(() -> in.fieldsReader(directory, si, fn, context));
                }

                @Override
                public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                    return writer(StoredFieldsWriter.class, () -> in.fieldsWriter(directory, si, context));
                }
            };
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            KnnVectorsFormat in = delegate.knnVectorsFormat();
            return format != Format.KNN_VECTORS ? in : new KnnVectorsFormat(in.getName()) {
                @Override
                public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                    return writer(KnnVectorsWriter.class, () -> in.fieldsWriter(state));
                }

                @Override
                public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
                    return reader(() -> in.fieldsReader(state));
                }

                @Override
                public int getMaxDimensions(String fieldName) {
                    return in.getMaxDimensions(fieldName);
                }
            };
        }

        @Override
        public PointsFormat pointsFormat() {
            PointsFormat in = delegate.pointsFormat();
            return format != Format.POINTS ? in : new PointsFormat() {
                @Override
                public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
                    return writer(PointsWriter.class, () -> in.fieldsWriter(state));
                }

                @Override
                public PointsReader fieldsReader(SegmentReadState state) throws IOException {
                    return reader(() -> in.fieldsReader(state));
                }
            };
        }

        @Override
        public NormsFormat normsFormat() {
            NormsFormat in = delegate.normsFormat();
            return format != Format.NORMS ? in : new NormsFormat() {
                @Override
                public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
                    return writer(NormsConsumer.class, () -> in.normsConsumer(state));
                }

                @Override
                public NormsProducer normsProducer(SegmentReadState state) throws IOException {
                    return reader(() -> in.normsProducer(state));
                }
            };
        }
    }
}
