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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * An IVF kNN query for byte-encoded vector fields. The raw byte[] query is passed directly to
 * the codec. When preconditioning is enabled, the query is preconditioned byte→byte before search.
 */
public class IVFKnnByteVectorQuery extends AbstractIVFKnnVectorQuery {

    protected final byte[] query;

    /**
     * Creates a new {@link IVFKnnByteVectorQuery}.
     * @param field the field to search
     * @param query the byte query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     * @param queryConfigResolver resolves per-segment IVF configuration
     */
    public IVFKnnByteVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver
    ) {
        this(field, query, k, numCands, filter, visitRatio, queryConfigResolver, false);
    }

    IVFKnnByteVectorQuery(
        String field,
        byte[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        boolean postFilterDelegate
    ) {
        super(field, visitRatio, k, numCands, filter, queryConfigResolver, postFilterDelegate);
        this.query = query;
    }

    public byte[] getQuery() {
        return query;
    }

    /** BYTE-encoded vectors: counting the FLOAT32 values of a byte field would yield 0 and silently
     *  disable post-filtering (see {@link KnnQueryUtils#countByteVectors}). */
    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        return KnnQueryUtils.countByteVectors(field, leaves);
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

    /**
     * Returns the query to search {@code context} with. When {@code usePrecondition} is set, the
     * segment's own preconditioner (segments are calibrated independently and each stores its own)
     * is applied to a fresh copy; the shared {@link #query} is never mutated. Falls back to the
     * original query if the segment has no preconditioner.
     */
    protected byte[] segmentQuery(LeafReaderContext context, boolean usePrecondition) throws IOException {
        if (usePrecondition == false) {
            return query;
        }
        LeafReader reader = context.reader();
        SegmentReader segmentReader = Lucene.tryUnwrapSegmentReader(reader);
        if (segmentReader == null) {
            return query;
        }
        KnnVectorsReader fieldsReader = segmentReader.getVectorReader();
        if (fieldsReader instanceof PerFieldKnnVectorsFormat.FieldsReader) {
            KnnVectorsReader knnVectorsReader = ((PerFieldKnnVectorsFormat.FieldsReader) fieldsReader).getFieldReader(field);
            if (knnVectorsReader instanceof VectorPreconditioner) {
                FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(field);
                Preconditioner preconditioner = ((VectorPreconditioner) knnVectorsReader).getPreconditioner(fieldInfo);
                if (preconditioner != null) {
                    byte[] out = new byte[query.length];
                    float[] scratch = new float[query.length];
                    preconditioner.applyTransformToBytes(query, out, scratch);
                    return out;
                }
            }
        }
        return query;
    }

    @Override
    TopDocs getLeafResults(
        LeafReaderContext ctx,
        Weight filterWeight,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        boolean usePrecondition
    ) throws IOException {
        AcceptDocs acceptDocs = leafAcceptDocs(ctx, filterWeight);
        if (acceptDocs == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }
        return approximateSearch(ctx, acceptDocs, Integer.MAX_VALUE, knnCollectorManager, visitRatio, segmentQuery(ctx, usePrecondition));
    }

    @Override
    boolean canSplitLeaves() {
        return getClass() == IVFKnnByteVectorQuery.class; // subclasses (sliced, nested) search their leaves whole
    }

    @Override
    PartSearch partSearch(LeafReaderContext ctx, Weight filterWeight, boolean usePrecondition) throws IOException {
        byte[] leafQuery = segmentQuery(ctx, usePrecondition);
        return collector -> {
            AcceptDocs acceptDocs = leafAcceptDocs(ctx, filterWeight);
            if (acceptDocs != null) {
                ctx.reader().searchNearestVectors(field, leafQuery, collector, acceptDocs);
            }
        };
    }

    @Override
    Query getAutoRescoreQuery(IndexSearcher indexSearcher, Query approxTopN, int finalK, int rescoreK) {
        return RescoreKnnVectorQuery.fromInnerQuery(field, query, finalK, rescoreK, approxTopN);
    }

    private TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        byte[] leafQuery
    ) throws IOException {
        LeafReader reader = context.reader();
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, numCands, k, knnCollectorManager.longAccumulator);
        AbstractMaxScoreKnnCollector knnCollector = knnCollectorManager.newCollector(visitedLimit, strategy, context);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);
        reader.searchNearestVectors(field, leafQuery, knnCollector, acceptDocs);
        TopDocs results = knnCollector instanceof BulkKnnCollector bulkKnnCollector
            ? bulkKnnCollector.unsortedTopK()
            : knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }
}
