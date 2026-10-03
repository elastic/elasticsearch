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
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecMetrics.Format;

import java.io.IOException;
import java.util.Collection;

/**
 * Records every failure thrown while writing or merging the data-bearing formats (postings, doc values, stored fields, vectors, points,
 * norms) into {@link CodecMetrics}. Keeps the delegate's name so {@code segments_N} still records the SPI-resolvable codec.
 * <p>
 * This mostly sees the write side. {@code IndexWriter} writes with the codec from its config and also opens the segments it flushed in
 * the same session through it (NRT readers, merge sources), so a failing open is counted here too. Segments from earlier sessions are
 * resolved by name via SPI ({@code SegmentInfos.readCommit} -> {@code Codec.forName}), so they never pass through this instance and, on
 * stateless search nodes, nothing does. Read failures are recorded by
 * {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader}, which every engine applies after Lucene has opened the segment.
 * Producers returned here are deliberately not wrapped so {@code instanceof} fast paths on them keep working.
 * <p>
 * Calls that are about one field pass its {@link FieldInfo} along so {@link Format#formatName} can name the concrete per-field format;
 * the per-field dispatcher stamps that name onto the field before handing the call to the format, so it is set by the time the format
 * fails. Calls spanning many fields (postings writes, merges, flush, close) record the format family instead. Only exceptions are
 * counted: a JVM error is not a codec failure, and recording it could throw again and mask it.
 */
public final class MetricingCodec extends FilterCodec {

    private final CodecMetrics metrics;
    private final PostingsFormat postingsFormat;
    private final DocValuesFormat docValuesFormat;
    private final StoredFieldsFormat storedFieldsFormat;
    private final KnnVectorsFormat knnVectorsFormat;
    private final PointsFormat pointsFormat;
    private final NormsFormat normsFormat;

    @SuppressWarnings("this-escape")
    public MetricingCodec(Codec delegate, CodecMetrics metrics) {
        super(delegate.getName(), delegate);
        this.metrics = metrics;
        this.postingsFormat = new MetricsPostingsFormat(delegate.postingsFormat());
        this.docValuesFormat = new MetricsDocValuesFormat(delegate.docValuesFormat());
        this.storedFieldsFormat = new MetricsStoredFieldsFormat(delegate.storedFieldsFormat());
        this.knnVectorsFormat = new MetricsKnnVectorsFormat(delegate.knnVectorsFormat());
        this.pointsFormat = new MetricsPointsFormat(delegate.pointsFormat());
        this.normsFormat = new MetricsNormsFormat(delegate.normsFormat());
    }

    public Codec delegate() {
        return delegate;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        return normsFormat;
    }

    /** Counts {@code e} against {@code format}, naming the concrete format from {@code field} if the failing call was about one field. */
    private void record(Format format, @Nullable FieldInfo field, Exception e) {
        metrics.onFailure(format, delegate, field, e);
    }

    /** Runs {@code call}, recording anything it throws under the given format before rethrowing it unchanged. */
    private <T> T runWithMetrics(Format format, IOSupplier<T> call) throws IOException {
        return runWithMetrics(format, null, call);
    }

    private void runWithMetrics(Format format, IORunnable call) throws IOException {
        runWithMetrics(format, null, call);
    }

    private <T> T runWithMetrics(Format format, @Nullable FieldInfo field, IOSupplier<T> call) throws IOException {
        try {
            return call.get();
        } catch (Exception e) {
            record(format, field, e);
            throw e;
        }
    }

    private void runWithMetrics(Format format, @Nullable FieldInfo field, IORunnable call) throws IOException {
        try {
            call.run();
        } catch (Exception e) {
            record(format, field, e);
            throw e;
        }
    }

    private final class MetricsPostingsFormat extends PostingsFormat {
        private final PostingsFormat in;

        MetricsPostingsFormat(PostingsFormat in) {
            super(in.getName());
            this.in = in;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new MetricsFieldsConsumer(runWithMetrics(Format.POSTINGS, () -> in.fieldsConsumer(state)));
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return runWithMetrics(Format.POSTINGS, () -> in.fieldsProducer(state));
        }
    }

    private final class MetricsFieldsConsumer extends FieldsConsumer {
        private final FieldsConsumer in;

        MetricsFieldsConsumer(FieldsConsumer in) {
            this.in = in;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            runWithMetrics(Format.POSTINGS, () -> in.write(fields, norms));
        }

        @Override
        public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
            runWithMetrics(Format.POSTINGS, () -> in.merge(mergeState, norms));
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.POSTINGS, in::close);
        }
    }

    private final class MetricsDocValuesFormat extends DocValuesFormat {
        private final DocValuesFormat in;

        MetricsDocValuesFormat(DocValuesFormat in) {
            super(in.getName());
            this.in = in;
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new MetricsDocValuesConsumer(runWithMetrics(Format.DOC_VALUES, () -> in.fieldsConsumer(state)));
        }

        @Override
        public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
            return runWithMetrics(Format.DOC_VALUES, () -> in.fieldsProducer(state));
        }
    }

    private final class MetricsDocValuesConsumer extends DocValuesConsumer {
        private final DocValuesConsumer in;

        MetricsDocValuesConsumer(DocValuesConsumer in) {
            this.in = in;
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            runWithMetrics(Format.DOC_VALUES, field, () -> in.addNumericField(field, valuesProducer));
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            runWithMetrics(Format.DOC_VALUES, field, () -> in.addBinaryField(field, valuesProducer));
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            runWithMetrics(Format.DOC_VALUES, field, () -> in.addSortedField(field, valuesProducer));
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            runWithMetrics(Format.DOC_VALUES, field, () -> in.addSortedNumericField(field, valuesProducer));
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            runWithMetrics(Format.DOC_VALUES, field, () -> in.addSortedSetField(field, valuesProducer));
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            runWithMetrics(Format.DOC_VALUES, () -> in.merge(mergeState));
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.DOC_VALUES, in::close);
        }
    }

    private final class MetricsStoredFieldsFormat extends StoredFieldsFormat {
        private final StoredFieldsFormat in;

        MetricsStoredFieldsFormat(StoredFieldsFormat in) {
            this.in = in;
        }

        @Override
        public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            return runWithMetrics(Format.STORED_FIELDS, () -> in.fieldsReader(directory, si, fn, context));
        }

        @Override
        public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
            return new MetricsStoredFieldsWriter(runWithMetrics(Format.STORED_FIELDS, () -> in.fieldsWriter(directory, si, context)));
        }
    }

    /**
     * The one per-document write path, so every call is wrapped by hand instead of through {@link #runWithMetrics}: no lambda allocation
     * per stored field.
     */
    private final class MetricsStoredFieldsWriter extends StoredFieldsWriter {
        private final StoredFieldsWriter in;

        MetricsStoredFieldsWriter(StoredFieldsWriter in) {
            this.in = in;
        }

        @Override
        public void startDocument() throws IOException {
            try {
                in.startDocument();
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void finishDocument() throws IOException {
            try {
                in.finishDocument();
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, StoredFieldDataInput value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            try {
                in.writeField(info, value);
            } catch (Exception e) {
                record(Format.STORED_FIELDS, null, e);
                throw e;
            }
        }

        @Override
        public void finish(int numDocs) throws IOException {
            runWithMetrics(Format.STORED_FIELDS, () -> in.finish(numDocs));
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            return runWithMetrics(Format.STORED_FIELDS, () -> in.merge(mergeState));
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.STORED_FIELDS, in::close);
        }

        @Override
        public long ramBytesUsed() {
            return in.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return in.getChildResources();
        }
    }

    private final class MetricsKnnVectorsFormat extends KnnVectorsFormat {
        private final KnnVectorsFormat in;

        MetricsKnnVectorsFormat(KnnVectorsFormat in) {
            super(in.getName());
            this.in = in;
        }

        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return new MetricsKnnVectorsWriter(runWithMetrics(Format.KNN_VECTORS, () -> in.fieldsWriter(state)));
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
            return runWithMetrics(Format.KNN_VECTORS, () -> in.fieldsReader(state));
        }

        @Override
        public int getMaxDimensions(String fieldName) {
            return in.getMaxDimensions(fieldName);
        }
    }

    private final class MetricsKnnVectorsWriter extends KnnVectorsWriter {
        private final KnnVectorsWriter in;

        MetricsKnnVectorsWriter(KnnVectorsWriter in) {
            this.in = in;
        }

        @Override
        public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
            return runWithMetrics(Format.KNN_VECTORS, fieldInfo, () -> in.addField(fieldInfo));
        }

        @Override
        public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
            runWithMetrics(Format.KNN_VECTORS, () -> in.flush(maxDoc, sortMap));
        }

        @Override
        public void finish() throws IOException {
            runWithMetrics(Format.KNN_VECTORS, in::finish);
        }

        @Override
        public IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
            IORunnable deferred = runWithMetrics(Format.KNN_VECTORS, fieldInfo, () -> in.mergeOneField(fieldInfo, mergeState));
            return deferred == null ? null : () -> runWithMetrics(Format.KNN_VECTORS, fieldInfo, deferred);
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.KNN_VECTORS, in::close);
        }

        @Override
        public long ramBytesUsed() {
            return in.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return in.getChildResources();
        }
    }

    private final class MetricsPointsFormat extends PointsFormat {
        private final PointsFormat in;

        MetricsPointsFormat(PointsFormat in) {
            this.in = in;
        }

        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
            return new MetricsPointsWriter(runWithMetrics(Format.POINTS, () -> in.fieldsWriter(state)));
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) throws IOException {
            return runWithMetrics(Format.POINTS, () -> in.fieldsReader(state));
        }
    }

    private final class MetricsPointsWriter extends PointsWriter {
        private final PointsWriter in;

        MetricsPointsWriter(PointsWriter in) {
            this.in = in;
        }

        @Override
        public void writeField(FieldInfo fieldInfo, PointsReader values) throws IOException {
            runWithMetrics(Format.POINTS, () -> in.writeField(fieldInfo, values));
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            runWithMetrics(Format.POINTS, () -> in.merge(mergeState));
        }

        @Override
        public void finish() throws IOException {
            runWithMetrics(Format.POINTS, in::finish);
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.POINTS, in::close);
        }
    }

    private final class MetricsNormsFormat extends NormsFormat {
        private final NormsFormat in;

        MetricsNormsFormat(NormsFormat in) {
            this.in = in;
        }

        @Override
        public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
            return new MetricsNormsConsumer(runWithMetrics(Format.NORMS, () -> in.normsConsumer(state)));
        }

        @Override
        public NormsProducer normsProducer(SegmentReadState state) throws IOException {
            return runWithMetrics(Format.NORMS, () -> in.normsProducer(state));
        }
    }

    private final class MetricsNormsConsumer extends NormsConsumer {
        private final NormsConsumer in;

        MetricsNormsConsumer(NormsConsumer in) {
            this.in = in;
        }

        @Override
        public void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException {
            runWithMetrics(Format.NORMS, () -> in.addNormsField(field, normsProducer));
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            runWithMetrics(Format.NORMS, () -> in.merge(mergeState));
        }

        @Override
        public void close() throws IOException {
            runWithMetrics(Format.NORMS, in::close);
        }
    }
}
