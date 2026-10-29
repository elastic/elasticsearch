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
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
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
import org.elasticsearch.index.codec.CodecMetrics.Format;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class MetricingCodecTests extends ESTestCase {

    public void testNameRoundTripsThroughSpi() {
        Codec delegate = TestUtil.getDefaultCodec();
        MetricingCodec codec = new MetricingCodec(delegate, CodecMetrics.NOOP);
        assertEquals(delegate.getName(), codec.getName());
        assertEquals(delegate.getClass(), Codec.forName(codec.getName()).getClass());
    }

    public void testCleanWriteAndMergeRecordNothing() throws IOException {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(TestUtil.getDefaultCodec(), new CodecMetrics(registry));
        try (Directory dir = newDirectory()) {
            writeSegments(dir, codec, between(2, 4));
            try (IndexWriter writer = new IndexWriter(dir, config(codec, false))) {
                writer.forceMerge(1);
            }
        }
        assertThat(failures(registry), empty());
    }

    /**
     * A failing flush is counted against the failing format, whether the format fails while opening, writing or closing. The failing
     * formats replace the per-field dispatchers, so no field carries a format name and the family label is recorded.
     */
    public void testWriteFailure() throws IOException {
        Format format = randomFrom(Format.values());
        Exception failure = randomFailure();
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec failing = new FailingCodec(format, randomFrom(FailAt.WRITER_OPEN, FailAt.WRITER, FailAt.WRITER_CLOSE), failure);
        Codec codec = new MetricingCodec(failing, new CodecMetrics(registry));

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
        assertFailure(registry, format.label(), failure);
    }

    /**
     * A failing merge is counted against the failing format, including the close that SegmentMerger issues after merge returns. Vectors
     * merge one field at a time and the merged field infos keep the format name the source segments were written with, so that is the
     * one merge failure that resolves to the concrete format; every other one records the family label.
     */
    public void testMergeFailure() throws IOException {
        Format format = randomFrom(Format.values());
        FailAt failAt = randomFrom(FailAt.WRITER_OPEN, FailAt.WRITER, FailAt.WRITER_CLOSE);
        Exception failure = randomFailure();
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(new FailingCodec(format, failAt, failure), new CodecMetrics(registry));

        try (Directory dir = newDirectory()) {
            writeSegments(dir, TestUtil.getDefaultCodec(), between(2, 4));
            IndexWriter writer = new IndexWriter(dir, config(codec, false));
            expectThrows(Exception.class, () -> writer.forceMerge(1));
            IOUtils.closeWhileHandlingException(writer);
        }
        String expectedFormat = format == Format.KNN_VECTORS && failAt == FailAt.WRITER
            ? ((PerFieldKnnVectorsFormat) TestUtil.getDefaultCodec().knnVectorsFormat()).getKnnVectorsFormatForField("vector")
                .getClass()
                .getSimpleName()
            : format.label();
        assertFailure(registry, expectedFormat, failure);
    }

    /** Segments flushed by the current writer are opened through its codec (NRT readers, merge sources), so a failing open is counted. */
    public void testOpenFailure() throws IOException {
        Format format = randomFrom(Format.values());
        Exception failure = randomFailure();
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Codec codec = new MetricingCodec(new FailingCodec(format, FailAt.READER_OPEN, failure), new CodecMetrics(registry));

        try (Directory dir = newDirectory()) {
            IndexWriter writer = new IndexWriter(dir, config(codec, true));
            writer.addDocument(randomDocument());
            expectThrows(Exception.class, () -> DirectoryReader.open(writer).close());
            IOUtils.closeWhileHandlingException(writer);
        }
        assertFailure(registry, format.label(), failure);
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

    private static void assertFailure(RecordingMeterRegistry registry, String expectedFormat, Exception failure) {
        List<Measurement> measurements = failures(registry);
        assertThat(measurements, hasSize(1));
        assertEquals(1L, measurements.get(0).getLong());
        assertEquals(
            Map.of(CodecMetrics.FORMAT_ATTRIBUTE, expectedFormat, MetricAttributes.ERROR_TYPE, failure.getClass().getSimpleName()),
            measurements.get(0).attributes()
        );
    }

    private static Exception randomFailure() {
        return randomFrom(new CorruptIndexException("corrupt", "test"), new IOException("io"), new IllegalStateException("state"));
    }

    /** Throws the failure; the return type only exists so callers can write {@code throw raise(failure)}. */
    private static IOException raise(Exception e) throws IOException {
        if (e instanceof IOException ioException) {
            throw ioException;
        }
        throw (RuntimeException) e;
    }

    private enum FailAt {
        WRITER_OPEN,
        WRITER,
        WRITER_CLOSE,
        READER_OPEN
    }

    /**
     * The default codec with one format replaced by a version that fails at the chosen point. Which writer method ends up failing is
     * decided by Lucene: a flush calls the add/write methods, a merge calls merge.
     */
    private static final class FailingCodec extends FilterCodec {
        private final Format format;
        private final FailAt failAt;
        private final Exception failure;

        FailingCodec(Format format, FailAt failAt, Exception failure) {
            super(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec());
            this.format = format;
            this.failAt = failAt;
            this.failure = failure;
        }

        /** The failing format's writer: fails to open, fails on every call except the bookkeeping ones, fails on close, or is real. */
        private <T> T writer(Class<T> type, IOSupplier<T> real) throws IOException {
            return switch (failAt) {
                case WRITER_OPEN -> throw raise(failure);
                case WRITER -> mock(type, invocation -> switch (invocation.getMethod().getName()) {
                    case "close" -> null;
                    case "ramBytesUsed" -> 0L;
                    case "getChildResources" -> List.of();
                    default -> throw raise(failure);
                });
                case WRITER_CLOSE -> {
                    // Like a real writer, only the first close fails: Lucene closes a flushed writer again when it aborts.
                    AtomicBoolean closed = new AtomicBoolean();
                    yield mock(type, invocation -> {
                        if (invocation.getMethod().getName().equals("close") && closed.getAndSet(true) == false) {
                            throw raise(failure);
                        }
                        return RETURNS_MOCKS.answer(invocation);
                    });
                }
                case READER_OPEN -> real.get();
            };
        }

        private <T> T reader(IOSupplier<T> real) throws IOException {
            if (failAt == FailAt.READER_OPEN) {
                throw raise(failure);
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
