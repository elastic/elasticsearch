/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link ParentChildIndexFieldData}.
 */
public class ParentQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentQuery that = (ParentQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ParentQuery[").append(parentType).append("](")
                .append(originalParentQuery.toString(field)).append(')')
                .append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewriteIndexReader = reader;
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenParentQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        ParentIdAndScoreCollector collector = new ParentIdAndScoreCollector(searchContext, parentChildIndexFieldData, parentType);

        final Query parentQuery;
        if (rewrittenParentQuery == null) {
            parentQuery = rewrittenParentQuery = searcher.rewrite(originalParentQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        indexSearcher.search(parentQuery, collector);
        FloatArray scores = collector.scores;
        BytesRefHash parentIds = collector.parentIds;

        if (parentIds.size() == 0) {
            Releasables.release(parentIds, scores);
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        ChildWeight childWeight = new ChildWeight(searchContext, parentQuery.createWeight(searcher), childrenFilter, parentIds, scores);
        searchContext.addReleasable(childWeight);
        return childWeight;
    }

    private static class ParentIdAndScoreCollector extends NoopCollector {

        private final BytesRefHash parentIds;
        private FloatArray scores;
        private final ParentChildIndexFieldData indexFieldData;
        private final String parentType;
        private final BigArrays bigArrays;

        private Scorer scorer;
        private BytesValues values;

        ParentIdAndScoreCollector(SearchContext searchContext, ParentChildIndexFieldData indexFieldData, String parentType) {
            this.bigArrays = searchContext.bigArrays();
            this.parentIds = new BytesRefHash(512, bigArrays);
            this.scores = bigArrays.newFloatArray(512, false);
            this.indexFieldData = indexFieldData;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (values != null) {
                values.setDocument(doc);
                long index = parentIds.add(values.nextValue(), values.currentValueHash());
                if (index >= 0) {
                    scores = bigArrays.grow(scores, index + 1);
                    scores.set(index, scorer.score());
                }
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(parentType);
        }
    }

    private class ChildWeight extends Weight implements Releasable {

        private final SearchContext searchContext;
        private final Weight parentWeight;
        private final Filter childrenFilter;
        private final BytesRefHash parentIds;
        private final FloatArray scores;

        private FixedBitSet seenOrdinalsCache;
        private LongArray parentIdsIndexCache;

        private ChildWeight(SearchContext searchContext, Weight parentWeight, Filter childrenFilter, BytesRefHash parentIds, FloatArray scores) {
            this.searchContext = searchContext;
            this.parentWeight = parentWeight;
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.parentIds = parentIds;
            this.scores = scores;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = parentWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocSet)) {
                return null;
            }
            BytesValues.WithOrdinals bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            Ordinals.Docs ordinals = bytesValues.ordinals();
            final int maxOrd = (int) ordinals.getMaxOrd();
            final BigArrays bigArrays = searchContext.bigArrays();
            if (parentIdsIndexCache == null) {
                parentIdsIndexCache = bigArrays.newLongArray(BigArrays.overSize(maxOrd), false);
            } else if (parentIdsIndexCache.size() < maxOrd) {
                parentIdsIndexCache = bigArrays.grow(parentIdsIndexCache, maxOrd);
            }
            parentIdsIndexCache.fill(0, maxOrd, -1L);
            if (seenOrdinalsCache == null || seenOrdinalsCache.length() < maxOrd) {
                seenOrdinalsCache = new FixedBitSet(maxOrd);
            } else {
                seenOrdinalsCache.clear(0, maxOrd);
            }
            return new ChildScorer(this, parentIds, scores, childrenDocSet.iterator(), bytesValues, ordinals, seenOrdinalsCache, parentIdsIndexCache);
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(parentIds, scores, parentIdsIndexCache);
            return true;
        }
    }

    private static class ChildScorer extends Scorer {

        private final BytesRefHash parentIds;
        private final FloatArray scores;
        private final DocIdSetIterator childrenIterator;
        private final BytesValues.WithOrdinals bytesValues;
        private final Ordinals.Docs ordinals;

        // This remembers what ordinals have already been seen in the current segment
        // and prevents from fetch the actual id from FD and checking if it exists in parentIds
        private final FixedBitSet seenOrdinals;
        private final LongArray parentIdsIndex;

        private int currentChildDoc = -1;
        private float currentScore;

        ChildScorer(Weight weight, BytesRefHash parentIds, FloatArray scores, DocIdSetIterator childrenIterator,
                    BytesValues.WithOrdinals bytesValues, Ordinals.Docs ordinals, FixedBitSet seenOrdinals, LongArray parentIdsIndex) {
            super(weight);
            this.parentIds = parentIds;
            this.scores = scores;
            this.childrenIterator = childrenIterator;
            this.bytesValues = bytesValues;
            this.ordinals = ordinals;
            this.seenOrdinals = seenOrdinals;
            this.parentIdsIndex = parentIdsIndex;
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        @Override
        public int freq() throws IOException {
            // We don't have the original child query hit info here...
            // But the freq of the children could be collector and returned here, but makes this Scorer more expensive.
            return 1;
        }

        @Override
        public int docID() {
            return currentChildDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            while (true) {
                currentChildDoc = childrenIterator.nextDoc();
                if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentChildDoc;
                }

                int ord = (int) ordinals.getOrd(currentChildDoc);
                if (ord == Ordinals.MISSING_ORDINAL) {
                    continue;
                }

                if (!seenOrdinals.get(ord)) {
                    seenOrdinals.set(ord);
                    long parentIdx = parentIds.find(bytesValues.getValueByOrd(ord), bytesValues.currentValueHash());
                    if (parentIdx != -1) {
                        currentScore = scores.get(parentIdx);
                        parentIdsIndex.set(ord, parentIdx);
                        return currentChildDoc;
                    }
                } else {
                    long parentIdx = parentIdsIndex.get(ord);
                    if (parentIdx != -1) {
                        currentScore = scores.get(parentIdx);
                        return currentChildDoc;
                    }
                }
            }
        }

        @Override
        public int advance(int target) throws IOException {
            currentChildDoc = childrenIterator.advance(target);
            if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                return currentChildDoc;
            }

            int ord = (int) ordinals.getOrd(currentChildDoc);
            if (ord == Ordinals.MISSING_ORDINAL) {
                return nextDoc();
            }

            if (!seenOrdinals.get(ord)) {
                seenOrdinals.set(ord);
                long parentIdx = parentIds.find(bytesValues.getValueByOrd(ord), bytesValues.currentValueHash());
                if (parentIdx != -1) {
                    currentScore = scores.get(parentIdx);
                    parentIdsIndex.set(ord, parentIdx);
                    return currentChildDoc;
                } else {
                    return nextDoc();
                }
            } else {
                long parentIdx = parentIdsIndex.get(ord);
                if (parentIdx != -1) {
                    currentScore = scores.get(parentIdx);
                    return currentChildDoc;
                } else {
                    return nextDoc();
                }
            }
        }

        @Override
        public long cost() {
            return childrenIterator.cost();
        }
    }
}
