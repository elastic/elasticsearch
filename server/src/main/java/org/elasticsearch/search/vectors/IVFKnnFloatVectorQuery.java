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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.VectorPreconditioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongSupplier;

/** A {@link IVFKnnFloatVectorQuery} that uses the IVF search strategy. */
public class IVFKnnFloatVectorQuery extends AbstractIVFKnnVectorQuery {

    protected final float[] query;

    /**
     * Creates a new {@link IVFKnnFloatVectorQuery} with the given parameters.
     * @param field the field to search
     * @param query the query vector
     * @param k the number of nearest neighbors to return (possibly oversampled)
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     */
    public IVFKnnFloatVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver
    ) {
        super(field, visitRatio, k, numCands, filter, queryConfigResolver);
        this.query = query;
    }

    public float[] getQuery() {
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
        IVFKnnFloatVectorQuery that = (IVFKnnFloatVectorQuery) o;
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
    protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) throws IOException {
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
                    final float[] out = new float[query.length];
                    preconditioner.applyTransform(query, out);
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
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();
        final int maxDoc = reader.maxDoc();
        final float[] leafQuery = segmentQuery(ctx, usePrecondition);

        if (filterWeight == null) {
            return approximateSearch(
                ctx,
                liveDocs == null ? new ESAcceptDocs.ESAcceptDocsAll() : new ESAcceptDocs.BitsAcceptDocs(liveDocs, maxDoc),
                Integer.MAX_VALUE,
                knnCollectorManager,
                visitRatio,
                leafQuery
            );
        }

        ScorerSupplier supplier = filterWeight.scorerSupplier(ctx);
        if (supplier == null) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }
        // no need to cache iterator as we are calling it once
        IOSupplier<DocIdSetIterator> docIdIteratorSupplier = () -> supplier.get(Long.MAX_VALUE).iterator();
        LongSupplier costSupplier = () -> supplier.cost();
        return approximateSearch(
            ctx,
            new ESAcceptDocs.ScorerSupplierAcceptDocs(docIdIteratorSupplier, costSupplier, liveDocs, maxDoc),
            Integer.MAX_VALUE,
            knnCollectorManager,
            visitRatio,
            leafQuery
        );
    }

    private TopDocs approximateSearch(
        LeafReaderContext context,
        AcceptDocs acceptDocs,
        int visitedLimit,
        IVFCollectorManager knnCollectorManager,
        float visitRatio,
        float[] leafQuery
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

    @Override
    Query getAutoRescoreQuery(IndexSearcher indexSearcher, TopDocs topOversampled, int effectiveK) {
        Query topDocsQuery = new KnnScoreDocQuery(topOversampled.scoreDocs, indexSearcher.getIndexReader());
        return RescoreKnnVectorQuery.fromInnerQuery(field, query, k, effectiveK, topDocsQuery);
    }
}
