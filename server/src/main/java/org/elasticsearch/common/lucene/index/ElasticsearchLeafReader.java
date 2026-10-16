/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReader;
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
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecMetrics;
import org.elasticsearch.index.codec.CodecMetrics.Format;
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

    /**
     * <p>Construct a FilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public ElasticsearchLeafReader(LeafReader in, ShardId shardId, CodecMetrics codecMetrics) {
        super(in);
        this.shardId = shardId;
        this.codecMetrics = codecMetrics;
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

    /**
     * Counts {@code e} against {@code format}. The segment's codec and the field's info, when the accessor was about one field, let
     * {@link Format#formatName} name the concrete format; both are looked up only here, on the failure path.
     */
    private void record(Format format, @Nullable String field, Exception e) {
        codecMetrics.onFailure(format, codec(), field == null ? null : in.getFieldInfos().fieldInfo(field), e);
    }

    /** The SPI codec the segment was written with, or null if {@link #in} does not wrap a segment (some test readers). */
    @Nullable
    private Codec codec() {
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(in);
        return segmentReader == null ? null : Codec.forName(segmentReader.getSegmentInfo().info.getCodec().getName());
    }

    /** Runs {@code call}, recording anything it throws under the given format and field before rethrowing it unchanged. */
    private <T> T runWithMetrics(Format format, @Nullable String field, IOSupplier<T> call) throws IOException {
        try {
            return call.get();
        } catch (Exception e) {
            record(format, field, e);
            throw e;
        }
    }

    private void runWithMetrics(Format format, @Nullable String field, IORunnable call) throws IOException {
        try {
            call.run();
        } catch (Exception e) {
            record(format, field, e);
            throw e;
        }
    }

    @Override
    public Terms terms(String field) throws IOException {
        return runWithMetrics(Format.POSTINGS, field, () -> in.terms(field));
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getNumericDocValues(field));
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getBinaryDocValues(field));
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getSortedDocValues(field));
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getSortedNumericDocValues(field));
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getSortedSetDocValues(field));
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        return runWithMetrics(Format.DOC_VALUES, field, () -> in.getDocValuesSkipper(field));
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return runWithMetrics(Format.NORMS, field, () -> in.getNormValues(field));
    }

    @Override
    public PointValues getPointValues(String field) throws IOException {
        return runWithMetrics(Format.POINTS, field, () -> in.getPointValues(field));
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return runWithMetrics(Format.KNN_VECTORS, field, () -> in.getFloatVectorValues(field));
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return runWithMetrics(Format.KNN_VECTORS, field, () -> in.getByteVectorValues(field));
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        runWithMetrics(Format.KNN_VECTORS, field, () -> in.searchNearestVectors(field, target, knnCollector, acceptDocs));
    }

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        runWithMetrics(Format.KNN_VECTORS, field, () -> in.searchNearestVectors(field, target, knnCollector, acceptDocs));
    }

    @Override
    public StoredFields storedFields() throws IOException {
        StoredFields storedFields = runWithMetrics(Format.STORED_FIELDS, null, in::storedFields);
        return new StoredFields() {
            @Override
            public void prefetch(int docID) throws IOException {
                try {
                    storedFields.prefetch(docID);
                } catch (Exception e) {
                    record(Format.STORED_FIELDS, null, e);
                    throw e;
                }
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                try {
                    storedFields.document(docID, visitor);
                } catch (Exception e) {
                    record(Format.STORED_FIELDS, null, e);
                    throw e;
                }
            }
        };
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }
}
