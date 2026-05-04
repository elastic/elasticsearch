/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;

/** A {@link IVFKnnFloatSlicedVectorQuery} that uses the IVF search strategy with an sliced index. */
public class IVFKnnFloatSlicedVectorQuery extends IVFKnnFloatVectorQuery {

    private final String sliceField;
    private final BytesRef sliceId;

    /**
     * Creates a new {@link IVFKnnFloatSlicedVectorQuery} with the given parameters.
     * @param field the field to search
     * @param query the query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     * @param sliceField the field used for slicing the index
     * @param sliceId the slice to be search
     */
    public IVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        String sliceField,
        BytesRef sliceId
    ) {
        super(field, query, k, numCands, filter, visitRatio, doPrecondition);
        this.sliceField = Objects.requireNonNull(sliceField);
        this.sliceId = Objects.requireNonNull(sliceId);
    }

    public IVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        String sliceField,
        BytesRef sliceId,
        Map<Integer, FixedBitSet> mergedSkip
    ) {
        this(field, query, k, numCands, filter, visitRatio, doPrecondition, sliceField, sliceId, mergedSkip, false);
    }

    IVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        boolean doPrecondition,
        String sliceField,
        BytesRef sliceId,
        Map<Integer, FixedBitSet> mergedSkip,
        boolean trackCentroidsForRetry
    ) {
        super(field, query, k, numCands, filter, visitRatio, doPrecondition, mergedSkip, trackCentroidsForRetry);
        this.sliceField = Objects.requireNonNull(sliceField);
        this.sliceId = Objects.requireNonNull(sliceId);
    }

    @Override
    TopDocs getLeafResults(LeafReaderContext ctx, Weight filterWeight, IVFCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        final LeafReader reader = ctx.reader();
        if (reader.numDocs() == 0) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }
        final Bits liveDocs = reader.getLiveDocs();
        final int maxDoc = reader.maxDoc();
        final Sort sort = reader.getMetaData().sort();
        if (sort == null
            || sort.getSort().length == 0
            || sort.getSort()[0].getField().equals(sliceField) == false
            || sort.getSort()[0].getType() != SortField.Type.STRING) {
            throw new IllegalArgumentException("sliceField must be the first field of the index sort and of type STRING");
        }

        final SortedDocValues sortedDocValues = ctx.reader().getSortedDocValues(sliceField);
        assert sortedDocValues != null : "sliceField must have doc values";
        final int sliceOrd = sortedDocValues.lookupTerm(sliceId);
        if (sliceOrd < 0) {
            return TopDocsCollector.EMPTY_TOPDOCS;
        }
        var skipper = ctx.reader().getDocValuesSkipper(sliceField);
        if (skipper == null) {
            throw new IllegalArgumentException("sliceField [" + sliceField + "] must be indexed as a DocValuesSkipper field");
        }
        if (skipper.docCount() != maxDoc) {
            throw new IllegalArgumentException(
                "DocValuesSkipper for sliceField [" + sliceField + "] must have a doc count equal to maxDoc"
            );
        }
        final IOSupplier<ESAcceptDocs.SliceAcceptDocs> sliceAcceptDocsSupplier = () -> getSliceAcceptDocsSupplier(
            sortedDocValues,
            skipper,
            sliceOrd
        );
        final AcceptDocs acceptDocs;
        if (filterWeight == null) {
            acceptDocs = liveDocs == null
                ? new ESAcceptDocs.ESAcceptDocsAll(sliceOrd, sliceAcceptDocsSupplier)
                : new ESAcceptDocs.BitsAcceptDocs(liveDocs, maxDoc, sliceOrd, sliceAcceptDocsSupplier);
        } else {
            ScorerSupplier supplier = filterWeight.scorerSupplier(ctx);
            if (supplier == null) {
                return TopDocsCollector.EMPTY_TOPDOCS;
            }
            acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(supplier, liveDocs, maxDoc, sliceOrd, sliceAcceptDocsSupplier);
        }
        return approximateSearch(ctx, acceptDocs, Integer.MAX_VALUE, knnCollectorManager, visitRatio);
    }

    @Override
    public int countTotalVectors(List<LeafReaderContext> leaves) throws IOException {
        int totalVectors = 0;
        for (LeafReaderContext leaf : leaves) {
            SortedDocValues sdv = leaf.reader().getSortedDocValues(sliceField);
            if (sdv == null) {
                continue;
            }
            int sliceOrd = sdv.lookupTerm(sliceId);
            if (sliceOrd < 0) {
                continue;
            }
            IVFVectorsReader<?> ivfReader = IVFVectorsReader.getIVFReader(leaf.reader(), field);
            if (ivfReader != null) {
                totalVectors += ivfReader.getTotalVectorsForSlice(field, sliceOrd);
            } else {
                FloatVectorValues fvv = leaf.reader().getFloatVectorValues(field);
                if (fvv != null) {
                    totalVectors += fvv.size();
                }
            }
        }
        return totalVectors;
    }

    @Override
    public IVFKnnFloatSlicedVectorQuery createPostFilterDelegate(float filterSelectivity) {
        // Round-1 K oversample: max of a 20% floor and the binomial-variance approximation.
        double zMargin = PostFilterableKnnQuery.zMargin(k, filterSelectivity);
        int scaledK = (int) Math.clamp(
            Math.ceil((k + zMargin) / filterSelectivity),
            Math.ceil(k * POST_FILTER_OVERSAMPLE_FLOOR),
            NUM_CANDS_LIMIT
        );
        // numCands and visit ratio share the scaledK/k multiplier (see IVFKnnFloatVectorQuery for rationale).
        int scaledNumCands = (int) Math.clamp(Math.ceil((double) scaledK * numCands / k), scaledK, NUM_CANDS_LIMIT);
        double oversampleMultiplier = (double) scaledK / k;
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, (float) (providedVisitRatio * oversampleMultiplier)) : 0f;
        return new IVFKnnFloatSlicedVectorQuery(
            field,
            originalQuery.clone(),
            scaledK,
            scaledNumCands,
            null,
            scaledVisitRatio,
            doPrecondition,
            sliceField,
            sliceId,
            null,
            true
        );
    }

    @Override
    public Query createRetryQuery(IndexReader reader, int[] excludedDocs, int[] seedDocs, int remainingK) {
        Map<Integer, FixedBitSet> skipCentroids = buildSkipCentroids();
        Query filter = excludedDocs != null && excludedDocs.length > 0 ? new ExcludeDocsQuery(excludedDocs, reader) : null;
        // Derive retry numCands from this query's k/numCands ratio so the IVF beam scales with retry K.
        int retryNumCands = (int) Math.clamp(Math.ceil((double) remainingK * numCands / k), remainingK, NUM_CANDS_LIMIT);
        // Widen visit ratio by POST_FILTER_OVERSAMPLE_FLOOR for the retry round.
        float scaledVisitRatio = providedVisitRatio > 0f ? Math.min(1.0f, providedVisitRatio * POST_FILTER_OVERSAMPLE_FLOOR) : 0f;
        return new IVFKnnFloatSlicedVectorQuery(
            field,
            originalQuery.clone(),
            remainingK,
            retryNumCands,
            filter,
            scaledVisitRatio,
            doPrecondition,
            sliceField,
            sliceId,
            skipCentroids
        );
    }

    private static ESAcceptDocs.SliceAcceptDocs getSliceAcceptDocsSupplier(
        SortedDocValues sortedDocValues,
        DocValuesSkipper skipper,
        int ord
    ) throws IOException {
        int minDocID;
        if (skipper.minValue() == ord) {
            minDocID = 0;
        } else {
            skipper.advance(ord, Long.MAX_VALUE);
            minDocID = nextDoc(skipper.minDocID(0), sortedDocValues, ord);
        }
        int maxDocID;
        if (skipper.maxValue() == ord) {
            maxDocID = skipper.docCount() - 1;
        } else {
            skipper.advance(ord + 1, Long.MAX_VALUE);
            maxDocID = nextDoc(skipper.minDocID(0), sortedDocValues, ord + 1) - 1;
        }
        return new ESAcceptDocs.SliceAcceptDocs(minDocID, maxDocID);
    }

    private static int nextDoc(int startDoc, SortedDocValues docValues, int ord) throws IOException {
        int doc = docValues.docID();
        if (startDoc > doc) {
            doc = docValues.advance(startDoc);
        }
        for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = docValues.nextDoc()) {
            if (ord == docValues.ordValue()) {
                break;
            }
        }
        return doc;
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(getClass().getSimpleName())
            .append(":")
            .append(this.field)
            .append("[")
            .append(getQuery()[0])
            .append(",...]")
            .append("[")
            .append(k)
            .append("]")
            .append("[")
            .append(sliceField)
            .append("=")
            .append(sliceId.utf8ToString())
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
        IVFKnnFloatSlicedVectorQuery that = (IVFKnnFloatSlicedVectorQuery) o;
        return Objects.equals(sliceField, that.sliceField) && Objects.equals(sliceId, that.sliceId);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hash(sliceField, sliceId);
        return result;
    }
}
