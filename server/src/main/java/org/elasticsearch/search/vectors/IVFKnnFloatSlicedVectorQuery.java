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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfQueryConfigResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongSupplier;

/** A {@link IVFKnnFloatSlicedVectorQuery} that uses the IVF search strategy with an sliced index. */
public class IVFKnnFloatSlicedVectorQuery extends IVFKnnFloatVectorQuery {

    private final String sliceField;
    private final BytesRef[] sliceIds;

    /**
     * Creates a new {@link IVFKnnFloatSlicedVectorQuery} with the given parameters.
     * @param field the field to search
     * @param query the query vector
     * @param k the number of nearest neighbors to return
     * @param numCands the number of nearest neighbors to gather per shard
     * @param filter the filter to apply to the results
     * @param visitRatio the ratio of vectors to score for the IVF search strategy
     * @param sliceField the field used for slicing the index
     * @param sliceIds the slices to be searched. If the array is empty, all slices are searched
     */
    public IVFKnnFloatSlicedVectorQuery(
        String field,
        float[] query,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        IvfQueryConfigResolver queryConfigResolver,
        String sliceField,
        BytesRef... sliceIds
    ) {
        super(field, query, k, numCands, filter, visitRatio, queryConfigResolver);
        this.sliceField = Objects.requireNonNull(sliceField);
        this.sliceIds = Objects.requireNonNull(sliceIds);
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

        final IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, numCands, k, knnCollectorManager.longAccumulator);
        final AbstractMaxScoreKnnCollector knnCollector = knnCollectorManager.newCollector(Integer.MAX_VALUE, strategy, ctx);
        if (knnCollector == null) {
            return NO_RESULTS;
        }
        strategy.setCollector(knnCollector);

        final SortedDocValues sortedDocValues = ctx.reader().getSortedDocValues(sliceField);
        if (sortedDocValues == null) {
            throw new IllegalArgumentException("sliceField [" + sliceField + "] must be indexed as a SortedDocValues field");
        }
        // Get ordinals sorted so we can share the iterator of the filter if it exists. Note that it means tht in case
        // of filters, we cannot process slices in parallel as the iterator needs to be consume in order.
        final int[] ords;
        if (sliceIds.length > 0) {
            ords = sliceToSortedOrds(sortedDocValues, sliceIds);
            if (ords.length == 0) {
                return NO_RESULTS;
            }
        } else {
            ords = null;
        }
        final DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(sliceField);
        if (skipper == null) {
            throw new IllegalArgumentException("sliceField [" + sliceField + "] must be indexed as a DocValuesSkipper field");
        }
        if (skipper.docCount() != maxDoc) {
            throw new IllegalArgumentException(
                "DocValuesSkipper for sliceField [" + sliceField + "] must have a doc count equal to maxDoc"
            );
        }

        final IOSupplier<DocIdSetIterator> docIdIteratorSupplier;
        final LongSupplier costSupplier;
        if (filterWeight != null) {
            ScorerSupplier supplier = filterWeight.scorerSupplier(ctx);
            if (supplier == null) {
                return NO_RESULTS;
            }
            docIdIteratorSupplier = new IOSupplier<>() {
                DocIdSetIterator cached = null;

                @Override
                public DocIdSetIterator get() throws IOException {
                    if (cached == null) {
                        cached = supplier.get(Long.MAX_VALUE).iterator();
                    }
                    return cached;
                }
            };
            costSupplier = supplier::cost;
        } else {
            docIdIteratorSupplier = null;
            costSupplier = null;
        }
        if (ords != null) {
            for (int i = 0; i < ords.length; i++) {
                assert i == 0 || ords[i - 1] < ords[i];
                approximateSearchForOneSlice(
                    sortedDocValues,
                    skipper,
                    ords[i],
                    knnCollector,
                    docIdIteratorSupplier,
                    costSupplier,
                    liveDocs,
                    maxDoc,
                    ctx
                );
            }
        } else {
            int numOrds = sortedDocValues.getValueCount();
            for (int i = 0; i < numOrds; i++) {
                approximateSearchForOneSlice(
                    sortedDocValues,
                    skipper,
                    i,
                    knnCollector,
                    docIdIteratorSupplier,
                    costSupplier,
                    liveDocs,
                    maxDoc,
                    ctx
                );
            }
        }
        TopDocs results = knnCollector instanceof BulkKnnCollector bulkKnnCollector
            ? bulkKnnCollector.unsortedTopK()
            : knnCollector.topDocs();
        return results != null ? results : NO_RESULTS;
    }

    private void approximateSearchForOneSlice(
        SortedDocValues sortedDocValues,
        DocValuesSkipper skipper,
        int sliceOrd,
        KnnCollector knnCollector,
        IOSupplier<DocIdSetIterator> docIdIteratorSupplier,
        LongSupplier costSupplier,
        Bits liveDocs,
        int maxDoc,
        LeafReaderContext context
    ) throws IOException {
        final IOSupplier<ESAcceptDocs.SliceAcceptDocs> sliceAcceptDocsSupplier = () -> getSliceAcceptDocsSupplier(
            sortedDocValues,
            skipper,
            sliceOrd
        );
        final AcceptDocs acceptDocs;
        if (docIdIteratorSupplier == null) {
            acceptDocs = liveDocs == null
                ? new ESAcceptDocs.ESAcceptDocsAll(sliceOrd, sliceAcceptDocsSupplier)
                : new ESAcceptDocs.BitsAcceptDocs(liveDocs, maxDoc, sliceOrd, sliceAcceptDocsSupplier);
        } else {
            acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(
                docIdIteratorSupplier,
                costSupplier,
                liveDocs,
                maxDoc,
                sliceOrd,
                sliceAcceptDocsSupplier
            );
        }
        context.reader().searchNearestVectors(field, query, knnCollector, acceptDocs);
    }

    private int[] sliceToSortedOrds(SortedDocValues sortedDocValues, BytesRef[] sliceIds) throws IOException {
        // no need to deduplicate at that should have been done at a higher level.
        IntArrayList ords = new IntArrayList();
        for (BytesRef sliceId : sliceIds) {
            int ord = sortedDocValues.lookupTerm(sliceId);
            if (ord >= 0) {
                ords.add(ord);
            }
        }
        return ords.sort().toArray();
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
            minDocID = skipper.minValue(0) == ord ? skipper.minDocID(0) : nextDoc(skipper.minDocID(0), sortedDocValues, ord);
        }
        int maxDocID;
        if (skipper.maxValue() == ord) {
            maxDocID = skipper.docCount();
        } else {
            int nextOrd = ord + 1;
            skipper.advance(nextOrd, Long.MAX_VALUE);
            maxDocID = skipper.minValue(0) == nextOrd ? skipper.minDocID(0) : nextDoc(skipper.minDocID(0), sortedDocValues, nextOrd);
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
            .append(toString(sliceIds))
            .append("]");
        if (this.filter != null) {
            buffer.append("[").append(this.filter).append("]");
        }
        return buffer.toString();
    }

    private static String toString(BytesRef[] sliceIds) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("[");
        for (int i = 0; i < sliceIds.length; i++) {
            if (i > 0) {
                buffer.append(",");
            }
            buffer.append(sliceIds[i].utf8ToString());
        }
        buffer.append("]");
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        IVFKnnFloatSlicedVectorQuery that = (IVFKnnFloatSlicedVectorQuery) o;
        return Objects.equals(sliceField, that.sliceField) && Arrays.equals(sliceIds, that.sliceIds);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hash(sliceField, Arrays.hashCode(sliceIds));
        return result;
    }
}
