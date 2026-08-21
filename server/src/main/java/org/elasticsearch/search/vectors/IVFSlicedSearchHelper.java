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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Package-private helper that contains the encoding-agnostic sliced search logic shared by
 * {@link IVFKnnFloatSlicedVectorQuery} and {@link IVFKnnByteSlicedVectorQuery}.
 */
final class IVFSlicedSearchHelper {

    private IVFSlicedSearchHelper() {}

    /**
     * Callback that performs the actual vector search for one slice. Implementations capture
     * the typed query vector (byte[] or float[]) and call the appropriate
     * {@code LeafReader.searchNearestVectors} overload.
     */
    @FunctionalInterface
    interface SliceSearcher {
        void search(LeafReaderContext ctx, String field, KnnCollector collector, AcceptDocs acceptDocs) throws IOException;
    }

    /**
     * Executes the sliced IVF search: validates the index sort, resolves slice ordinals,
     * iterates slices, and collects results. The actual per-slice vector search is delegated
     * to {@code sliceSearcher}.
     */
    static TopDocs getLeafResults(
        LeafReaderContext ctx,
        Weight filterWeight,
        AbstractIVFKnnVectorQuery.IVFCollectorManager knnCollectorManager,
        float visitRatio,
        int numCands,
        int k,
        String field,
        String sliceField,
        BytesRef[] sliceIds,
        SliceSearcher sliceSearcher
    ) throws IOException {
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
            return AbstractIVFKnnVectorQuery.NO_RESULTS;
        }
        strategy.setCollector(knnCollector);

        final SortedDocValues sortedDocValues = ctx.reader().getSortedDocValues(sliceField);
        if (sortedDocValues == null) {
            throw new IllegalArgumentException("sliceField [" + sliceField + "] must be indexed as a SortedDocValues field");
        }
        // Get ordinals sorted so we can share the iterator of the filter if it exists. Note that it means that in case
        // of filters, we cannot process slices in parallel as the iterator needs to be consumed in order.
        final int[] ords;
        if (sliceIds.length > 0) {
            ords = sliceToSortedOrds(sortedDocValues, sliceIds);
            if (ords.length == 0) {
                return AbstractIVFKnnVectorQuery.NO_RESULTS;
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
                return AbstractIVFKnnVectorQuery.NO_RESULTS;
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
                searchOneSlice(
                    sortedDocValues,
                    skipper,
                    ords[i],
                    knnCollector,
                    docIdIteratorSupplier,
                    costSupplier,
                    liveDocs,
                    maxDoc,
                    ctx,
                    field,
                    sliceSearcher
                );
            }
        } else {
            int numOrds = sortedDocValues.getValueCount();
            for (int i = 0; i < numOrds; i++) {
                searchOneSlice(
                    sortedDocValues,
                    skipper,
                    i,
                    knnCollector,
                    docIdIteratorSupplier,
                    costSupplier,
                    liveDocs,
                    maxDoc,
                    ctx,
                    field,
                    sliceSearcher
                );
            }
        }
        TopDocs results = knnCollector instanceof BulkKnnCollector bulkKnnCollector
            ? bulkKnnCollector.unsortedTopK()
            : knnCollector.topDocs();
        return results != null ? results : AbstractIVFKnnVectorQuery.NO_RESULTS;
    }

    /**
     * Slice-restricted version of {@link KnnQueryUtils#computeSelectivity}: a sliced query only ever visits
     * the requested slices, so estimating its filter selectivity against the whole reader measures a corpus
     * it will not touch.
     */
    static float estimateSliceFilterSelectivity(
        List<LeafReaderContext> leaves,
        Weight filterWeight,
        String vectorField,
        boolean byteEncoded,
        String sliceField,
        BytesRef[] sliceIds
    ) throws IOException {
        double filterCost = 0;
        long sliceVectors = 0;
        for (LeafReaderContext ctx : leaves) {
            LeafReader reader = ctx.reader();
            int maxDoc = reader.maxDoc();
            if (maxDoc == 0) {
                continue;
            }
            KnnVectorValues values = byteEncoded ? reader.getByteVectorValues(vectorField) : reader.getFloatVectorValues(vectorField);
            if (values == null || values.size() == 0) {
                continue;
            }
            long sliceDocs = sliceDocCount(ctx, sliceField, sliceIds);
            if (sliceDocs <= 0) {
                continue;
            }
            double sliceShare = Math.min(1.0, (double) sliceDocs / maxDoc);
            // Vectors are not necessarily dense over the slice, so cap by the leaf's actual vector count.
            sliceVectors += Math.min(values.size(), (long) Math.ceil(values.size() * sliceShare));
            ScorerSupplier supplier = filterWeight.scorerSupplier(ctx);
            if (supplier != null) {
                filterCost += supplier.cost() * sliceShare;
            }
        }
        if (sliceVectors <= 0) {
            return 0f;
        }
        return (float) Math.min(1.0, filterCost / sliceVectors);
    }

    /**
     * Number of docs in {@code ctx} that belong to the requested slices, or {@code maxDoc} when no slice ids
     * were given (the query searches every slice). Uses the {@code sliceField} skipper, so this is O(number
     * of requested slices) rather than a doc walk.
     */
    private static long sliceDocCount(LeafReaderContext ctx, String sliceField, BytesRef[] sliceIds) throws IOException {
        int maxDoc = ctx.reader().maxDoc();
        if (sliceIds.length == 0) {
            return maxDoc;
        }
        SortedDocValues sortedDocValues = ctx.reader().getSortedDocValues(sliceField);
        DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper(sliceField);
        if (sortedDocValues == null || skipper == null || skipper.docCount() != maxDoc) {
            // Not a sliced leaf in the shape getLeafResults requires; fall back to the whole leaf rather
            // than reporting a bogus zero. getLeafResults will raise the real error at search time.
            return maxDoc;
        }
        long docs = 0;
        for (int ord : sliceToSortedOrds(sortedDocValues, sliceIds)) {
            ESAcceptDocs.SliceAcceptDocs range = getSliceAcceptDocsSupplier(sortedDocValues, skipper, ord);
            docs += Math.max(0, range.endDoc() - range.startDoc());
        }
        return docs;
    }

    private static void searchOneSlice(
        SortedDocValues sortedDocValues,
        DocValuesSkipper skipper,
        int sliceOrd,
        KnnCollector knnCollector,
        IOSupplier<DocIdSetIterator> docIdIteratorSupplier,
        LongSupplier costSupplier,
        Bits liveDocs,
        int maxDoc,
        LeafReaderContext context,
        String field,
        SliceSearcher sliceSearcher
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
        sliceSearcher.search(context, field, knnCollector, acceptDocs);
    }

    static int[] sliceToSortedOrds(SortedDocValues sortedDocValues, BytesRef[] sliceIds) throws IOException {
        // no need to deduplicate as that should have been done at a higher level.
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
}
