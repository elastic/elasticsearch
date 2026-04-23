/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;

import java.io.IOException;
import java.util.Arrays;

/**
 * An IVF kNN query for byte-encoded vector fields. The byte query vector is passed directly
 * to the codec's {@code search(field, byte[], ...)} method, which handles byte-to-float
 * conversion and COSINE normalization internally in {@code IVFVectorsReader}.
 */
public class IVFKnnByteVectorQuery extends AbstractIVFKnnVectorQuery {

    private final byte[] query;
    private boolean isQueryPreconditioned = false;
    private float[] preconditionedQuery;

    /**
     * Creates a new {@link IVFKnnByteVectorQuery}.
     * @param field the field to search
     * @param query the byte query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     * @param doPrecondition whether to apply preconditioning
     */
    public IVFKnnByteVectorQuery(String field, byte[] query, int k, int numCands, Query filter, float visitRatio, boolean doPrecondition) {
        super(field, visitRatio, k, numCands, filter, doPrecondition);
        this.query = query;
    }

    public byte[] getQuery() {
        return query;
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName())
            .append(":")
            .append(this.field)
            .append("[")
            .append(query[0])
            .append(",...]")
            .append("[")
            .append(k)
            .append("]");
        if (this.filter != null) {
            buffer.append("[").append(this.filter).append("]");
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IVFKnnByteVectorQuery that = (IVFKnnByteVectorQuery) o;
        return Arrays.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(query);
        return result;
    }

    @Override
    protected void preconditionQuery(LeafReaderContext context) throws IOException {
        if (isQueryPreconditioned) {
            return;
        }
        LeafReader reader = context.reader();
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            return;
        }
        KnnVectorsReader fieldsReader = segmentReader.getVectorReader();
        if (fieldsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            KnnVectorsReader knnVectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) fieldsReader).getFieldReader(field);
            if (knnVectorsReader instanceof VectorPreconditioner) {
                FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(field);
                Preconditioner preconditioner = ((VectorPreconditioner) knnVectorsReader).getPreconditioner(fieldInfo);
                if (preconditioner != null) {
                    // Convert byte query to float for preconditioning, then store the preconditioned float query.
                    float[] floatQuery = new float[query.length];
                    for (int i = 0; i < query.length; i++) {
                        floatQuery[i] = query[i];
                    }
                    // For COSINE, the IVF pipeline indexes L2-normalized byte-to-float vectors (then preconditioned).
                    // The query must be normalized to match before applying the same rotation.
                    if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
                        VectorUtil.l2normalize(floatQuery);
                    }
                    float[] out = new float[query.length];
                    preconditioner.applyTransform(floatQuery, out);
                    preconditionedQuery = out;
                    isQueryPreconditioned = true;
                }
            }
        }
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio
    ) throws IOException {
        LeafReader reader = context.reader();
        ByteVectorValues byteVectorValues = reader.getByteVectorValues(field);
        if (byteVectorValues == null) {
            ByteVectorValues.checkField(reader, field);
            return NO_RESULTS;
        }
        if (byteVectorValues.size() == 0) {
            return NO_RESULTS;
        }
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, numCands, k, knnCollectorManager.longAccumulator);
        AbstractMaxScoreKnnCollector knnCollector = knnCollectorManager.newCollector(visitedLimit, strategy, context);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        if (isQueryPreconditioned) {
            SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
            if (segmentReader != null) {
                KnnVectorsReader fieldsReader = segmentReader.getVectorReader();
                if (fieldsReader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader) {
                    KnnVectorsReader knnVectorsReader = perFieldReader.getFieldReader(field);
                    if (knnVectorsReader != null) {
                        knnVectorsReader.search(field, preconditionedQuery, knnCollector, acceptDocs);
                    }
                } else if (fieldsReader != null) {
                    fieldsReader.search(field, preconditionedQuery, knnCollector, acceptDocs);
                }
            }
        } else {
            reader.searchNearestVectors(field, query, knnCollector, acceptDocs);
        }
        TopDocs results = knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }
}
