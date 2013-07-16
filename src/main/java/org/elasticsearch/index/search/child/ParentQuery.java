/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.child;

import org.elasticsearch.common.lucene.docset.DocIdSets;

import gnu.trove.map.hash.TObjectFloatHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link IdReaderTypeCache}.
 */
// TODO We use a score of 0 to indicate a doc was not scored in uidToScore, this means score of 0 can be problematic, if we move to HPCC, we can use lset/...
public class ParentQuery extends Query implements SearchContext.Rewrite {

    private final SearchContext searchContext;
    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private TObjectFloatHashMap<HashedBytesArray> uidToScore;

    public ParentQuery(SearchContext searchContext, Query parentQuery, String parentType, Filter childrenFilter) {
        this.searchContext = searchContext;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
        uidToScore = searchContext.cacheRecycler().popObjectFloatMap();
        ParentUidCollector collector = new ParentUidCollector(uidToScore, searchContext, parentType);
        Query parentQuery;
        if (rewrittenParentQuery == null) {
            parentQuery = rewrittenParentQuery = searchContext.searcher().rewrite(originalParentQuery);
        } else {
            parentQuery = rewrittenParentQuery;
        }
        searchContext.searcher().search(parentQuery, collector);
    }

    @Override
    public void contextClear() {
        if (uidToScore != null) {
            searchContext.cacheRecycler().pushObjectFloatMap(uidToScore);
        }
        uidToScore = null;
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
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
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
        if (uidToScore == null) {
            throw new ElasticSearchIllegalStateException("has_parent query hasn't executed properly");
        }
        if (uidToScore.isEmpty()) {
            return Queries.NO_MATCH_QUERY.createWeight(searcher);
        }

        return new ChildWeight(rewrittenParentQuery.createWeight(searcher));
    }

    static class ParentUidCollector extends NoopCollector {

        final TObjectFloatHashMap<HashedBytesArray> uidToScore;
        final SearchContext searchContext;
        final String parentType;

        Scorer scorer;
        IdReaderTypeCache typeCache;

        ParentUidCollector(TObjectFloatHashMap<HashedBytesArray> uidToScore, SearchContext searchContext, String parentType) {
            this.uidToScore = uidToScore;
            this.searchContext = searchContext;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (typeCache == null) {
                return;
            }

            HashedBytesArray parentUid = typeCache.idByDoc(doc);
            uidToScore.put(parentUid, scorer.score());
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            typeCache = searchContext.idCache().reader(context.reader()).type(parentType);
        }
    }

    class ChildWeight extends Weight {

        private final Weight parentWeight;

        ChildWeight(Weight parentWeight) {
            this.parentWeight = parentWeight;
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
            IdReaderTypeCache idTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
            if (idTypeCache == null) {
                return null;
            }

            return new ChildScorer(this, uidToScore, childrenDocSet.iterator(), idTypeCache);
        }
    }

    static class ChildScorer extends Scorer {

        final TObjectFloatHashMap<HashedBytesArray> uidToScore;
        final DocIdSetIterator childrenIterator;
        final IdReaderTypeCache typeCache;

        int currentChildDoc = -1;
        float currentScore;

        ChildScorer(Weight weight, TObjectFloatHashMap<HashedBytesArray> uidToScore, DocIdSetIterator childrenIterator, IdReaderTypeCache typeCache) {
            super(weight);
            this.uidToScore = uidToScore;
            this.childrenIterator = childrenIterator;
            this.typeCache = typeCache;
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

                BytesReference uid = typeCache.parentIdByDoc(currentChildDoc);
                if (uid == null) {
                    continue;
                }
                currentScore = uidToScore.get(uid);
                if (currentScore != 0) {
                    return currentChildDoc;
                }
            }
        }

        @Override
        public int advance(int target) throws IOException {
            currentChildDoc = childrenIterator.advance(target);
            if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                return currentChildDoc;
            }
            BytesReference uid = typeCache.idByDoc(currentChildDoc);
            if (uid == null) {
                return nextDoc();
            }
            currentScore = uidToScore.get(uid);
            if (currentScore == 0) {
                return nextDoc();
            }
            return currentChildDoc;
        }

        @Override
        public long cost() {
            return childrenIterator.cost();
        }
    }
}
