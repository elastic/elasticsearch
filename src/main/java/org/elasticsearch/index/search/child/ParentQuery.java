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

import gnu.trove.map.hash.TObjectFloatHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link IdReaderTypeCache}.
 */
public class ParentQuery extends Query implements ScopePhase.CollectorPhase {

    private final SearchContext searchContext;
    private final Query parentQuery;
    private final String parentType;
    private final Filter childrenFilter;
    private final List<String> childTypes;
    private final String scope;

    private TObjectFloatHashMap<HashedBytesArray> uidToScore;

    public ParentQuery(SearchContext searchContext, Query parentQuery, String parentType, List<String> childTypes, Filter childrenFilter, String scope) {
        this.searchContext = searchContext;
        this.parentQuery = parentQuery;
        this.parentType = parentType;
        this.childTypes = childTypes;
        this.childrenFilter = childrenFilter;
        this.scope = scope;
    }

    private ParentQuery(ParentQuery unwritten, Query rewrittenParentQuery) {
        this.searchContext = unwritten.searchContext;
        this.parentQuery = rewrittenParentQuery;
        this.parentType = unwritten.parentType;
        this.childrenFilter = unwritten.childrenFilter;
        this.childTypes = unwritten.childTypes;
        this.scope = unwritten.scope;

        this.uidToScore = unwritten.uidToScore;
    }

    @Override
    public boolean requiresProcessing() {
        return uidToScore == null;
    }

    @Override
    public Collector collector() {
        uidToScore = CacheRecycler.popObjectFloatMap();
        return new ParentUidCollector(uidToScore, searchContext, parentType);
    }

    @Override
    public void processCollector(Collector collector) {
        // Do nothing, we already have the references to the parent scores.
    }

    @Override
    public String scope() {
        return scope;
    }

    @Override
    public void clear() {
        if (uidToScore != null) {
            CacheRecycler.pushObjectFloatMap(uidToScore);
        }
        uidToScore = null;
    }

    @Override
    public Query query() {
        return parentQuery;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        HasParentFilter that = (HasParentFilter) obj;
        if (!parentQuery.equals(that.parentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = parentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ParentQuery[").append(parentType).append("/").append(childTypes)
                .append("](").append(parentQuery.toString(field)).append(')')
                .append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewrittenChildQuery = parentQuery.rewrite(reader);
        if (rewrittenChildQuery == parentQuery) {
            return this;
        }
        ParentQuery rewrite = new ParentQuery(this, rewrittenChildQuery);
        int index = searchContext.scopePhases().indexOf(this);
        searchContext.scopePhases().set(index, rewrite);
        return rewrite;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        parentQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        if (uidToScore == null) {
            throw new ElasticSearchIllegalStateException("has_parent query hasn't executed properly");
        }
        return new ChildWeight(parentQuery.createWeight(searcher));
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
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
            typeCache = searchContext.idCache().reader(reader).type(parentType);
        }
    }

    class ChildWeight extends Weight {

        private final Weight parentWeight;

        ChildWeight(Weight parentWeight) {
            this.parentWeight = parentWeight;
        }

        @Override
        public Explanation explain(IndexReader reader, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentQuery.this;
        }

        @Override
        public float getValue() {
            return getBoost();
        }

        @Override
        public float sumOfSquaredWeights() throws IOException {
            float sum = parentWeight.sumOfSquaredWeights();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm) {
        }

        @Override
        public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            DocIdSet childrenDocSet = childrenFilter.getDocIdSet(reader);
            if (childrenDocSet == null || childrenDocSet == DocIdSet.EMPTY_DOCIDSET) {
                return null;
            }
            IdReaderTypeCache idTypeCache = searchContext.idCache().reader(reader).type(parentType);
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
        public float freq() throws IOException {
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
                if (Float.compare(currentScore, 0) != 0) {
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
            if (Float.compare(currentScore, 0) == 0) {
                return nextDoc();
            }
            return currentChildDoc;
        }
    }
}
