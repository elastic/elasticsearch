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

import gnu.trove.set.hash.THashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class HasChildFilter extends Filter implements SearchContext.Rewrite {

    final Query childQuery;
    final String scope;
    final String parentType;
    final String childType;
    final SearchContext searchContext;

    protected HasChildFilter(Query childQuery, String scope, String parentType, String childType, SearchContext searchContext) {
        this.searchContext = searchContext;
        this.parentType = parentType;
        this.childType = childType;
        this.scope = scope;
        this.childQuery = childQuery;
    }

    public Query query() {
        return childQuery;
    }

    public String scope() {
        return scope;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(childQuery).append(')');
        return sb.toString();
    }

    public static HasChildFilter create(Query childQuery, String scope, String parentType, String childType, SearchContext searchContext, String executionType) {
        // This mechanism is experimental and will most likely be removed.
        if ("bitset".equals(executionType)) {
            return new Bitset(childQuery, scope, parentType, childType, searchContext);
        } else if ("uid".endsWith(executionType)) {
            return new Uid(childQuery, scope, parentType, childType, searchContext);
        }
        throw new ElasticSearchIllegalStateException("Illegal has_child execution type: " + executionType);
    }

    static class Bitset extends HasChildFilter {

        private Map<Object, FixedBitSet> parentDocs;

        public Bitset(Query childQuery, String scope, String parentType, String childType, SearchContext searchContext) {
            super(childQuery, scope, parentType, childType, searchContext);
        }

        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            if (parentDocs == null) {
                throw new ElasticSearchIllegalStateException("has_child filter hasn't executed properly");
            }

            // np need to use acceptDocs, since the parentDocs were collected with a collector, which means those
            // collected docs are not deleted
            // ok to return null
            return parentDocs.get(context.reader().getCoreCacheKey());
        }

        @Override
        public void contextRewrite(SearchContext searchContext) throws Exception {
            searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
            ChildCollector collector = new ChildCollector(parentType, searchContext);
            searchContext.searcher().search(childQuery, collector);
            this.parentDocs = collector.parentDocs();
        }

        @Override
        public void contextClear() {
            parentDocs = null;
        }
    }

    static class Uid extends HasChildFilter {

        THashSet<HashedBytesArray> collectedUids;

        Uid(Query childQuery, String scope, String parentType, String childType, SearchContext searchContext) {
            super(childQuery, scope, parentType, childType, searchContext);
        }

        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            if (collectedUids == null) {
                throw new ElasticSearchIllegalStateException("has_child filter hasn't executed properly");
            }

            IdReaderTypeCache idReaderTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
            if (idReaderTypeCache != null) {
                return new ParentDocSet(context.reader(), acceptDocs, collectedUids, idReaderTypeCache);
            } else {
                return null;
            }
        }

        @Override
        public void contextRewrite(SearchContext searchContext) throws Exception {
            searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
            collectedUids = CacheRecycler.popHashSet();
            UidCollector collector = new UidCollector(parentType, searchContext, collectedUids);
            searchContext.searcher().search(childQuery, collector);
        }

        @Override
        public void contextClear() {
            if (collectedUids != null) {
                CacheRecycler.pushHashSet(collectedUids);
            }
            collectedUids = null;
        }

        static class ParentDocSet extends MatchDocIdSet {

            final IndexReader reader;
            final THashSet<HashedBytesArray> parents;
            final IdReaderTypeCache typeCache;

            ParentDocSet(IndexReader reader, @Nullable Bits acceptDocs, THashSet<HashedBytesArray> parents, IdReaderTypeCache typeCache) {
                super(reader.maxDoc(), acceptDocs);
                this.reader = reader;
                this.parents = parents;
                this.typeCache = typeCache;
            }

            @Override
            protected boolean matchDoc(int doc) {
                return parents.contains(typeCache.idByDoc(doc));
            }
        }

        static class UidCollector extends NoopCollector {

            final String parentType;
            final SearchContext context;
            final THashSet<HashedBytesArray> collectedUids;

            private IdReaderTypeCache typeCache;

            UidCollector(String parentType, SearchContext context, THashSet<HashedBytesArray> collectedUids) {
                this.parentType = parentType;
                this.context = context;
                this.collectedUids = collectedUids;
            }

            @Override
            public void collect(int doc) throws IOException {
                // It can happen that for particular segment no document exist for an specific type. This prevents NPE
                if (typeCache != null) {
                    collectedUids.add(typeCache.parentIdByDoc(doc));
                }

            }

            @Override
            public void setNextReader(AtomicReaderContext readerContext) throws IOException {
                typeCache = context.idCache().reader(readerContext.reader()).type(parentType);
            }
        }
    }
}
