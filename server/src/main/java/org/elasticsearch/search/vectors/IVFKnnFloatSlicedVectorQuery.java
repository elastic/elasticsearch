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
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

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
        this.sliceField = sliceField;
        this.sliceId = sliceId;
    }

    @Override
    TopDocs getLeafResults(LeafReaderContext ctx, Weight filterWeight, IVFCollectorManager knnCollectorManager, float visitRatio)
        throws IOException {
        final LeafReader reader = ctx.reader();
        final Bits liveDocs = reader.getLiveDocs();
        final int maxDoc = reader.maxDoc();

        AcceptDocs acceptDocs;
        final int sliceOrd;
        final IOSupplier<ESAcceptDocs.SliceAcceptDocs> sliceAcceptDocsSupplier;
        if (sliceId != null) {
            var sortedDocValues = ctx.reader().getSortedDocValues(sliceField);
            if (sortedDocValues != null) {
                sliceOrd = sortedDocValues.lookupTerm(sliceId);
                if (sliceOrd < 0) {
                    return TopDocsCollector.EMPTY_TOPDOCS;
                }
                var skipper = ctx.reader().getDocValuesSkipper(sliceField);
                assert skipper.docCount() == maxDoc; // dense
                sliceAcceptDocsSupplier = () -> getSliceAcceptDocsSupplier(sortedDocValues, skipper, sliceOrd);
            } else {
                sliceOrd = -1;
                sliceAcceptDocsSupplier = null;
            }
        } else {
            sliceOrd = -1;
            sliceAcceptDocsSupplier = null;
        }
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

    private ESAcceptDocs.SliceAcceptDocs getSliceAcceptDocsSupplier(SortedDocValues sortedDocValues, DocValuesSkipper skipper, int ord)
        throws IOException {
        int minDocID;
        if (skipper.minValue() == ord) {
            minDocID = 0;
        } else {
            skipper.advance(ord, Long.MAX_VALUE);
            minDocID = nextDoc(skipper.minDocID(0), sortedDocValues, ord);
        }
        int maxDocID;
        if (skipper.maxValue() == ord) {
            maxDocID = skipper.docCount();
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
            .append(Arrays.toString(getQuery()))
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
