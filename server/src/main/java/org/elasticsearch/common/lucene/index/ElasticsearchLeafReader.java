/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
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
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.index.codec.CodecMetrics.Format;
import org.elasticsearch.index.codec.CodecMetrics.Operation;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.FilterLeafReader} that exposes
 * Elasticsearch internal per shard / index information like the shard ID.
 * <p>
 * It is also the read-side hook for {@link CodecMetrics}: every engine applies it after Lucene has opened the segment through the
 * SPI-resolved codec, so it sees reads that a codec wrapper cannot. Failures thrown by the per-field accessors and {@link #storedFields()}
 * are counted; the iterators those accessors return and the sequential stored fields reader are not wrapped, so failures there escape
 * to the operation that drives them.
 */
public final class ElasticsearchLeafReader extends SequentialStoredFieldsLeafReader {

    private final ShardId shardId;
    private final CodecMetrics codecMetrics;
    private final IndexMode indexMode;

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public ElasticsearchLeafReader(LeafReader in, ShardId shardId, CodecMetrics codecMetrics, IndexMode indexMode) {
        super(in);
        this.shardId = shardId;
        this.codecMetrics = codecMetrics;
        this.indexMode = indexMode;
    }

    /**
     * Returns the shard id this segment belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public static ElasticsearchLeafReader getElasticsearchLeafReader(LeafReader reader) {
        if (reader instanceof FilterLeafReader) {
            if (reader instanceof ElasticsearchLeafReader) {
                return (ElasticsearchLeafReader) reader;
            } else {
                // We need to use FilterLeafReader#getDelegate and not FilterLeafReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of ElasticsearchLeafReader. This can cause us to miss the shardId.
                return getElasticsearchLeafReader(((FilterLeafReader) reader).getDelegate());
            }
        }
        return null;
    }

    private void record(Format format, Operation operation, Throwable t) {
        codecMetrics.onFailure(indexMode, format, operation, t);
    }

    /** Runs {@code call}, recording anything it throws under the given format and operation before rethrowing it unchanged. */
    private <T> T runWithMetrics(Format format, Operation operation, IOSupplier<T> call) throws IOException {
        try {
            return call.get();
        } catch (Throwable t) {
            record(format, operation, t);
            throw t;
        }
    }

    private void runWithMetrics(Format format, Operation operation, IORunnable call) throws IOException {
        try {
            call.run();
        } catch (Throwable t) {
            record(format, operation, t);
            throw t;
        }
    }

    @Override
    public Terms terms(String field) throws IOException {
        return runWithMetrics(Format.POSTINGS, Operation.READ, () -> in.terms(field));
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getNumericDocValues(field));
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getBinaryDocValues(field));
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getSortedDocValues(field));
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getSortedNumericDocValues(field));
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getSortedSetDocValues(field));
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, Operation.READ, () -> in.getDocValuesSkipper(field));
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return runWithMetrics(Format.NORMS, Operation.READ, () -> in.getNormValues(field));
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
        return runWithMetrics(Format.POINTS, Operation.READ, () -> in.getPointValues(field));
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return runWithMetrics(Format.KNN_VECTORS, Operation.READ, () -> in.getFloatVectorValues(field));
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return runWithMetrics(Format.KNN_VECTORS, Operation.READ, () -> in.getByteVectorValues(field));
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        runWithMetrics(Format.KNN_VECTORS, Operation.READ, () -> in.searchNearestVectors(field, target, knnCollector, acceptDocs));
    }

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        runWithMetrics(Format.KNN_VECTORS, Operation.READ, () -> in.searchNearestVectors(field, target, knnCollector, acceptDocs));
    }

    /** The per-document fetch path, so the two calls are wrapped by hand instead of through {@link #runWithMetrics}: no lambda per document. */
    @Override
    public StoredFields storedFields() throws IOException {
        StoredFields storedFields = runWithMetrics(Format.STORED_FIELDS, Operation.READ, in::storedFields);
        return new StoredFields() {
            @Override
            public void prefetch(int docID) throws IOException {
                try {
                    storedFields.prefetch(docID);
                } catch (Throwable t) {
                    record(Format.STORED_FIELDS, Operation.READ, t);
                    throw t;
                }
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                try {
                    storedFields.document(docID, visitor);
                } catch (Throwable t) {
                    record(Format.STORED_FIELDS, Operation.READ, t);
                    throw t;
                }
            }
        };
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
