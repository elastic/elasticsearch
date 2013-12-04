/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.RecyclerUtils;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class ChildrenConstantScoreQuery extends Query {

    private final Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenConstantScoreQuery(Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        searchContext.idCache().refresh(searcher.getTopReaderContext().leaves());
        Recycler.V<ObjectOpenHashSet<HashedBytesArray>> collectedUids = searchContext.cacheRecycler().hashSet(-1);
        UidCollector collector = new UidCollector(parentType, searchContext, collectedUids.v());
        final Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(childQuery, collector);

        int remaining = collectedUids.v().size();
        if (remaining == 0) {
            return Queries.NO_MATCH_QUERY.createWeight(searcher);
        }

        Filter shortCircuitFilter = null;
        if (remaining == 1) {
            BytesRef id = collectedUids.v().iterator().next().value.toBytesRef();
            shortCircuitFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
        } else if (remaining <= shortCircuitParentDocSet) {
            shortCircuitFilter = new ParentIdsFilter(parentType, collectedUids.v().keys, collectedUids.v().allocated, nonNestedDocsFilter);
        }

        ParentWeight parentWeight = new ParentWeight(parentFilter, shortCircuitFilter, searchContext, collectedUids);
        searchContext.addReleasable(parentWeight);
        return parentWeight;
    }

    private final class ParentWeight extends Weight implements Releasable  {

        private final Filter parentFilter;
        private final Filter shortCircuitFilter;
        private final SearchContext searchContext;
        private final Recycler.V<ObjectOpenHashSet<HashedBytesArray>> collectedUids;

        private int remaining;
        private float queryNorm;
        private float queryWeight;

        public ParentWeight(Filter parentFilter, Filter shortCircuitFilter, SearchContext searchContext, Recycler.V<ObjectOpenHashSet<HashedBytesArray>> collectedUids) {
            this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
            this.shortCircuitFilter = shortCircuitFilter;
            this.searchContext = searchContext;
            this.collectedUids = collectedUids;
            this.remaining = collectedUids.v().size();
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenConstantScoreQuery.this;
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
            if (remaining == 0) {
                return null;
            }

            if (shortCircuitFilter != null) {
                DocIdSet docIdSet = shortCircuitFilter.getDocIdSet(context, acceptDocs);
                if (!DocIdSets.isEmpty(docIdSet)) {
                    DocIdSetIterator iterator = docIdSet.iterator();
                    if (iterator != null) {
                        return ConstantScorer.create(iterator, this, queryWeight);
                    }
                }
                return null;
            }

            DocIdSet parentDocIdSet = this.parentFilter.getDocIdSet(context, acceptDocs);
            if (!DocIdSets.isEmpty(parentDocIdSet)) {
                IdReaderTypeCache idReaderTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
                // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
                // count down (short circuit) logic will then work as expected.
                parentDocIdSet = BitsFilteredDocIdSet.wrap(parentDocIdSet, context.reader().getLiveDocs());
                if (idReaderTypeCache != null) {
                    DocIdSetIterator innerIterator = parentDocIdSet.iterator();
                    if (innerIterator != null) {
                        ParentDocIdIterator parentDocIdIterator = new ParentDocIdIterator(innerIterator, collectedUids.v(), idReaderTypeCache);
                        return ConstantScorer.create(parentDocIdIterator, this, queryWeight);
                    }
                }
            }
            return null;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            RecyclerUtils.release(collectedUids);
            return true;
        }

        private final class ParentDocIdIterator extends FilteredDocIdSetIterator {

            private final ObjectOpenHashSet<HashedBytesArray> parents;
            private final IdReaderTypeCache typeCache;

            private ParentDocIdIterator(DocIdSetIterator innerIterator, ObjectOpenHashSet<HashedBytesArray> parents, IdReaderTypeCache typeCache) {
                super(innerIterator);
                this.parents = parents;
                this.typeCache = typeCache;
            }

            @Override
            protected boolean match(int doc) {
                if (remaining == 0) {
                    try {
                        advance(DocIdSetIterator.NO_MORE_DOCS);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }

                boolean match = parents.contains(typeCache.idByDoc(doc));
                if (match) {
                    remaining--;
                }
                return match;
            }
        }
    }

    private final static class UidCollector extends ParentIdCollector {

        private final ObjectOpenHashSet<HashedBytesArray> collectedUids;

        UidCollector(String parentType, SearchContext context, ObjectOpenHashSet<HashedBytesArray> collectedUids) {
            super(parentType, context);
            this.collectedUids = collectedUids;
        }

        @Override
        public void collect(int doc, HashedBytesArray parentIdByDoc) {
            collectedUids.add(parentIdByDoc);
        }

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenConstantScoreQuery that = (ChildrenConstantScoreQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (shortCircuitParentDocSet != that.shortCircuitParentDocSet) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
        result += shortCircuitParentDocSet;
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery).append(')');
        return sb.toString();
    }

}
