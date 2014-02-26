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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public class ParentConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
            rewriteIndexReader = reader;
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
        BytesRefHash parentIds = new BytesRefHash(512, searchContext.bigArrays());
        ParentIdsCollector collector = new ParentIdsCollector(parentType, parentChildIndexFieldData, parentIds);

        final Query parentQuery;
        if (rewrittenParentQuery != null) {
            parentQuery = rewrittenParentQuery;
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery = originalParentQuery.rewrite(searcher.getIndexReader());
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        indexSearcher.search(parentQuery, collector);

        if (parentIds.size() == 0) {
            Releasables.release(parentIds);
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        ChildrenWeight childrenWeight = new ChildrenWeight(childrenFilter, parentIds);
        searchContext.addReleasable(childrenWeight);
        return childrenWeight;
    }

    private final class ChildrenWeight extends Weight implements Releasable {

        private final Filter childrenFilter;
        private final BytesRefHash parentIds;

        private float queryNorm;
        private float queryWeight;

        private FixedBitSet seenOrdinalsCache;
        private FixedBitSet seenMatchedOrdinalsCache;

        private ChildrenWeight(Filter childrenFilter, BytesRefHash parentIds) {
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.parentIds = parentIds;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentConstantScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            queryWeight = getBoost();
            return queryWeight * queryWeight;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocIdSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocIdSet)) {
                return null;
            }

            BytesValues.WithOrdinals bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues != null) {
                DocIdSetIterator innerIterator = childrenDocIdSet.iterator();
                if (innerIterator != null) {
                    Ordinals.Docs ordinals = bytesValues.ordinals();
                    int maxOrd = (int) ordinals.getMaxOrd();
                    if (seenOrdinalsCache == null || seenOrdinalsCache.length() < maxOrd) {
                        seenOrdinalsCache = new FixedBitSet(maxOrd);
                        seenMatchedOrdinalsCache = new FixedBitSet(maxOrd);
                    } else {
                        seenOrdinalsCache.clear(0, maxOrd);
                        seenMatchedOrdinalsCache.clear(0, maxOrd);
                    }
                    ChildrenDocIdIterator childrenDocIdIterator = new ChildrenDocIdIterator(
                            innerIterator, parentIds, bytesValues, ordinals, seenOrdinalsCache, seenMatchedOrdinalsCache
                    );
                    return ConstantScorer.create(childrenDocIdIterator, this, queryWeight);
                }
            }
            return null;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(parentIds);
            return true;
        }

        private final class ChildrenDocIdIterator extends FilteredDocIdSetIterator {

            private final BytesRefHash parentIds;
            private final BytesValues.WithOrdinals bytesValues;
            private final Ordinals.Docs ordinals;

            // This remembers what ordinals have already been emitted in the current segment
            // and prevents from fetch the actual id from FD and checking if it exists in parentIds
            private final FixedBitSet seenOrdinals;
            private final FixedBitSet seenMatchedOrdinals;

            ChildrenDocIdIterator(DocIdSetIterator innerIterator, BytesRefHash parentIds, BytesValues.WithOrdinals bytesValues, Ordinals.Docs ordinals, FixedBitSet seenOrdinals, FixedBitSet seenMatchedOrdinals) {
                super(innerIterator);
                this.parentIds = parentIds;
                this.bytesValues = bytesValues;
                this.ordinals = ordinals;
                this.seenOrdinals = seenOrdinals;
                this.seenMatchedOrdinals = seenMatchedOrdinals;
            }

            @Override
            protected boolean match(int doc) {
                int ord = (int) ordinals.getOrd(doc);
                if (ord == Ordinals.MISSING_ORDINAL) {
                    return false;
                }

                if (!seenOrdinals.get(ord)) {
                    seenOrdinals.set(ord);
                    if (parentIds.find(bytesValues.getValueByOrd(ord), bytesValues.currentValueHash()) >= 0) {
                        seenMatchedOrdinals.set(ord);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return seenMatchedOrdinals.get(ord);
                }
            }

        }
    }

    private final static class ParentIdsCollector extends NoopCollector {

        private final BytesRefHash parentIds;
        private final ParentChildIndexFieldData indexFieldData;
        private final String parentType;

        private BytesValues values;

        ParentIdsCollector(String parentType, ParentChildIndexFieldData indexFieldData, BytesRefHash parentIds) {
            this.parentIds = parentIds;
            this.indexFieldData = indexFieldData;
            this.parentType = parentType;
        }

        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (values != null) {
                values.setDocument(doc);
                parentIds.add(values.nextValue(), values.currentValueHash());
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            values = indexFieldData.load(readerContext).getBytesValues(parentType);
        }
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentConstantScoreQuery that = (ParentConstantScoreQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (this.getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("parent_filter[").append(parentType).append("](").append(originalParentQuery).append(')');
        return sb.toString();
    }

}

