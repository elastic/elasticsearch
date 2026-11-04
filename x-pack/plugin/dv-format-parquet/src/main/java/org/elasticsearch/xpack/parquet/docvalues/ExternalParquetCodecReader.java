/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A reader over a file piece that is not in the index, with one document for each row. It exists to be handed to
 * {@code IndexWriter#addIndexes(CodecReader...)}, which turns it into a stub segment: the writer asks the doc values consumer
 * to merge the reader's producer, and the consumer (see {@link ParquetDocValuesConsumer}) copies the reference and reads no value.
 *
 * <p>The reader has no stored fields, postings, norms, points or vectors. The stored fields reader is a no-op that returns no field
 * for any document.
 */
public final class ExternalParquetCodecReader extends CodecReader {

    private final ParquetDocValuesProducer producer;
    private final FieldInfos fieldInfos;
    private final int maxDoc;

    public ExternalParquetCodecReader(List<ColumnBinding> bindings, List<ExternalFilePiece> pieces) throws IOException {
        this.producer = new ParquetDocValuesProducer(bindings, pieces, null);
        this.maxDoc = producer.maxDoc();
        FieldInfo[] infos = new FieldInfo[bindings.size()];
        for (int i = 0; i < infos.length; i++) {
            ColumnBinding binding = bindings.get(i);
            infos[i] = new FieldInfo(
                binding.fieldName(),
                binding.fieldNumber(),
                false,
                true,
                false,
                IndexOptions.NONE,
                binding.type(),
                skipIndexType(binding, pieces),
                -1,
                Map.of(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
        }
        this.fieldInfos = new FieldInfos(infos);
    }

    /**
     * A numeric field gets a skip index, which {@link ParquetDocValuesSkipper} serves from the footer statistics, when the snapshots
     * tell how many values each row group has. Without that count the skipper cannot say how many documents have a value, and the field
     * gets none.
     */
    private static DocValuesSkipIndexType skipIndexType(ColumnBinding binding, List<ExternalFilePiece> pieces) {
        if (binding.isSynthesized() || binding.type() != DocValuesType.NUMERIC) {
            return DocValuesSkipIndexType.NONE;
        }
        for (ExternalFilePiece piece : pieces) {
            ColumnSnapshot column = piece.column(binding.parquetColumnPath());
            if (column == null || column.hasExactValueCounts() == false) {
                return DocValuesSkipIndexType.NONE;
            }
        }
        return DocValuesSkipIndexType.RANGE;
    }

    @Override
    public StoredFieldsReader getFieldsReader() {
        return NoStoredFieldsReader.INSTANCE;
    }

    @Override
    public TermVectorsReader getTermVectorsReader() {
        return null;
    }

    @Override
    public NormsProducer getNormsReader() {
        return null;
    }

    @Override
    public DocValuesProducer getDocValuesReader() {
        return producer;
    }

    @Override
    public FieldsProducer getPostingsReader() {
        return null;
    }

    @Override
    public PointsReader getPointsReader() {
        return null;
    }

    @Override
    public KnnVectorsReader getVectorReader() {
        return null;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    @Override
    public Bits getLiveDocs() {
        return null;
    }

    @Override
    public int numDocs() {
        return maxDoc;
    }

    @Override
    public int maxDoc() {
        return maxDoc;
    }

    @Override
    public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    @Override
    protected void doClose() {}

    /** Returns no field for any document. */
    private static final class NoStoredFieldsReader extends StoredFieldsReader {
        static final NoStoredFieldsReader INSTANCE = new NoStoredFieldsReader();

        @Override
        public void document(int docID, StoredFieldVisitor visitor) {}

        @Override
        public StoredFieldsReader clone() {
            return this;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public void close() {}
    }
}
