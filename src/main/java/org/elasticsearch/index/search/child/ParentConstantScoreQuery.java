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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.RecyclerUtils;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public class ParentConstantScoreQuery extends Query {

    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentConstantScoreQuery(Query parentQuery, String parentType, Filter childrenFilter) {
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
        searchContext.idCache().refresh(searcher.getTopReaderContext().leaves());
        Recycler.V<ObjectOpenHashSet<HashedBytesArray>> parents = searchContext.cacheRecycler().hashSet(-1);
        ParentUidsCollector collector = new ParentUidsCollector(parents.v(), searchContext, parentType);

        final Query parentQuery;
        if (rewrittenParentQuery != null) {
            parentQuery = rewrittenParentQuery;
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery = originalParentQuery.rewrite(searcher.getIndexReader());
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(parentQuery, collector);

        if (parents.v().isEmpty()) {
            return Queries.NO_MATCH_QUERY.createWeight(searcher);
        }

        ChildrenWeight childrenWeight = new ChildrenWeight(childrenFilter, searchContext, parents);
        searchContext.addReleasable(childrenWeight);
        return childrenWeight;
    }

    private final class ChildrenWeight extends Weight implements Releasable {

        private final Filter childrenFilter;
        private final SearchContext searchContext;
        private final Recycler.V<ObjectOpenHashSet<HashedBytesArray>> parents;

        private float queryNorm;
        private float queryWeight;

        private ChildrenWeight(Filter childrenFilter, SearchContext searchContext, Recycler.V<ObjectOpenHashSet<HashedBytesArray>> parents) {
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.searchContext = searchContext;
            this.parents = parents;
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

            IdReaderTypeCache idReaderTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
            if (idReaderTypeCache != null) {
                DocIdSetIterator innerIterator = childrenDocIdSet.iterator();
                if (innerIterator != null) {
                    ChildrenDocIdIterator childrenDocIdIterator = new ChildrenDocIdIterator(innerIterator, parents.v(), idReaderTypeCache);
                    return ConstantScorer.create(childrenDocIdIterator, this, queryWeight);
                }
            }
            return null;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            RecyclerUtils.release(parents);
            return true;
        }

        private final class ChildrenDocIdIterator extends FilteredDocIdSetIterator {

            private final ObjectOpenHashSet<HashedBytesArray> parents;
            private final IdReaderTypeCache idReaderTypeCache;

            ChildrenDocIdIterator(DocIdSetIterator innerIterator, ObjectOpenHashSet<HashedBytesArray> parents, IdReaderTypeCache idReaderTypeCache) {
                super(innerIterator);
                this.parents = parents;
                this.idReaderTypeCache = idReaderTypeCache;
            }

            @Override
            protected boolean match(int doc) {
                return parents.contains(idReaderTypeCache.parentIdByDoc(doc));
            }

        }
    }

    private final static class ParentUidsCollector extends NoopCollector {

        private final ObjectOpenHashSet<HashedBytesArray> collectedUids;
        private final SearchContext context;
        private final String parentType;

        private IdReaderTypeCache typeCache;

        ParentUidsCollector(ObjectOpenHashSet<HashedBytesArray> collectedUids, SearchContext context, String parentType) {
            this.collectedUids = collectedUids;
            this.context = context;
            this.parentType = parentType;
        }

        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (typeCache != null) {
                collectedUids.add(typeCache.idByDoc(doc));
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            typeCache = context.idCache().reader(readerContext.reader()).type(parentType);
        }
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
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
        return true;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("parent_filter[").append(parentType).append("](").append(originalParentQuery).append(')');
        return sb.toString();
    }

}

