/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.index.codec.CodecMetrics.Format;
import org.elasticsearch.index.codec.CodecMetrics.Operation;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.hasSize;

public class ElasticsearchLeafReaderTests extends ESTestCase {

    private record Accessor(String name, Format format, Operation operation, CheckedConsumer<ElasticsearchLeafReader, IOException> call) {}

    private static final List<Accessor> ACCESSORS = List.of(
        new Accessor("terms", Format.POSTINGS, Operation.READ, r -> r.terms("f")),
        new Accessor("getNumericDocValues", Format.DOC_VALUES, Operation.READ, r -> r.getNumericDocValues("f")),
        new Accessor("getBinaryDocValues", Format.DOC_VALUES, Operation.READ, r -> r.getBinaryDocValues("f")),
        new Accessor("getSortedDocValues", Format.DOC_VALUES, Operation.READ, r -> r.getSortedDocValues("f")),
        new Accessor("getSortedNumericDocValues", Format.DOC_VALUES, Operation.READ, r -> r.getSortedNumericDocValues("f")),
        new Accessor("getSortedSetDocValues", Format.DOC_VALUES, Operation.READ, r -> r.getSortedSetDocValues("f")),
        new Accessor("getDocValuesSkipper", Format.DOC_VALUES, Operation.READ, r -> r.getDocValuesSkipper("f")),
        new Accessor("getNormValues", Format.NORMS, Operation.READ, r -> r.getNormValues("f")),
        new Accessor("getPointValues", Format.POINTS, Operation.READ, r -> r.getPointValues("f")),
        new Accessor("getFloatVectorValues", Format.KNN_VECTORS, Operation.READ, r -> r.getFloatVectorValues("f")),
        new Accessor("getByteVectorValues", Format.KNN_VECTORS, Operation.READ, r -> r.getByteVectorValues("f")),
        new Accessor(
            "searchNearestVectors(float)",
            Format.KNN_VECTORS,
            Operation.READ,
            r -> r.searchNearestVectors(
                "f",
                new float[] { 1f },
                new TopKnnCollector(1, Integer.MAX_VALUE),
                AcceptDocs.fromLiveDocs(null, 1)
            )
        ),
        new Accessor(
            "searchNearestVectors(byte)",
            Format.KNN_VECTORS,
            Operation.READ,
            r -> r.searchNearestVectors("f", new byte[] { 1 }, new TopKnnCollector(1, Integer.MAX_VALUE), AcceptDocs.fromLiveDocs(null, 1))
        ),
        new Accessor("storedFields().document", Format.STORED_FIELDS, Operation.READ, r -> r.storedFields().document(0)),
        new Accessor("storedFields().prefetch", Format.STORED_FIELDS, Operation.READ, r -> r.storedFields().prefetch(0))
    );

    public void testFailuresAreCountedPerFormat() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("f", randomAlphaOfLength(5), Field.Store.YES));
                writer.addDocument(doc);
            }
            for (Accessor accessor : ACCESSORS) {
                Failure failure = randomFailure();
                IndexMode indexMode = randomFrom(IndexMode.values());
                RecordingMeterRegistry registry = new RecordingMeterRegistry();
                try (
                    DirectoryReader reader = ElasticsearchDirectoryReader.wrap(
                        new ThrowingDirectoryReader(DirectoryReader.open(dir), failure),
                        new ShardId("index", "_na_", 0),
                        null,
                        new CodecMetrics(registry),
                        indexMode
                    )
                ) {
                    ElasticsearchLeafReader leaf = (ElasticsearchLeafReader) reader.leaves().get(0).reader();
                    expectThrows(Exception.class, () -> accessor.call().accept(leaf));
                }
                List<Measurement> measurements = registry.getRecorder()
                    .getMeasurements(InstrumentType.LONG_COUNTER, CodecMetrics.CODEC_FAILURE_TOTAL);
                assertThat(accessor.name(), measurements, hasSize(1));
                assertEquals(
                    accessor.name(),
                    Map.of(
                        CodecMetrics.INDEX_MODE_ATTRIBUTE,
                        indexMode.getName(),
                        MetricAttributes.ERROR_TYPE,
                        failure.expectedErrorType(),
                        CodecMetrics.FORMAT_ATTRIBUTE,
                        accessor.format().name().toLowerCase(Locale.ROOT),
                        CodecMetrics.OPERATION_ATTRIBUTE,
                        accessor.operation().name().toLowerCase(Locale.ROOT)
                    ),
                    measurements.get(0).attributes()
                );
            }
        }
    }

    private record Failure(Supplier<Exception> supplier, String expectedErrorType) {
        IOException raise() throws IOException {
            Exception e = supplier.get();
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            throw (RuntimeException) e;
        }
    }

    /** Includes wrapped failures so both branches of the shared {@code error_type} unwrapping are exercised. */
    private static Failure randomFailure() {
        return randomFrom(
            new Failure(() -> new CorruptIndexException("corrupt", "test"), "CorruptIndexException"),
            new Failure(() -> new IOException("io"), "IOException"),
            new Failure(() -> new IllegalStateException("state"), "IllegalStateException"),
            new Failure(() -> new UncheckedIOException(new CorruptIndexException("corrupt", "test")), "CorruptIndexException"),
            new Failure(() -> new RemoteTransportException("wrapped", new IOException("io")), "IOException")
        );
    }

    private static final class ThrowingDirectoryReader extends FilterDirectoryReader {
        private final Failure failure;

        ThrowingDirectoryReader(DirectoryReader in, Failure failure) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new ThrowingLeafReader(reader, failure);
                }
            });
            this.failure = failure;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new ThrowingDirectoryReader(in, failure);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /** Fails every codec-backed accessor, so the test only has to pick which one to call. */
    private static final class ThrowingLeafReader extends FilterLeafReader {
        private final Failure failure;

        ThrowingLeafReader(LeafReader in, Failure failure) {
            super(in);
            this.failure = failure;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        public Terms terms(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            throw failure.raise();
        }

        @Override
        public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
            throws IOException {
            throw failure.raise();
        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            throw failure.raise();
        }

        @Override
        public StoredFields storedFields() {
            return new StoredFields() {
                @Override
                public void prefetch(int docID) throws IOException {
                    throw failure.raise();
                }

                @Override
                public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                    throw failure.raise();
                }
            };
        }
    }
}
