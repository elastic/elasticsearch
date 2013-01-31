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

import gnu.trove.set.hash.THashSet;
import org.apache.lucene.index.AtomicReader;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * A filter that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public abstract class HasParentFilter extends Filter implements SearchContext.Rewrite {

    final Query parentQuery;
    final String scope;
    final String parentType;
    final SearchContext context;

    HasParentFilter(Query parentQuery, String scope, String parentType, SearchContext context) {
        this.parentQuery = parentQuery;
        this.scope = scope;
        this.parentType = parentType;
        this.context = context;
    }

    public String scope() {
        return scope;
    }

    public Query query() {
        return parentQuery;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("parent_filter[").append(parentType).append("](").append(query()).append(')');
        return sb.toString();
    }

    public static HasParentFilter create(String executionType, Query query, String scope, String parentType, SearchContext context) {
        // This mechanism is experimental and will most likely be removed.
        if ("bitset".equals(executionType)) {
            return new Bitset(query, scope, parentType, context);
        } else if ("uid".equals(executionType)) {
            return new Uid(query, scope, parentType, context);
        }
        throw new ElasticSearchIllegalStateException("Illegal has_parent execution type: " + executionType);
    }

    static class Uid extends HasParentFilter {

        THashSet<HashedBytesArray> parents;

        Uid(Query query, String scope, String parentType, SearchContext context) {
            super(query, scope, parentType, context);
        }

        public DocIdSet getDocIdSet(AtomicReaderContext readerContext, Bits acceptDocs) throws IOException {
            if (parents == null) {
                throw new ElasticSearchIllegalStateException("has_parent filter hasn't executed properly");
            }

            IdReaderTypeCache idReaderTypeCache = context.idCache().reader(readerContext.reader()).type(parentType);
            if (idReaderTypeCache != null) {
                return new ChildrenDocSet(readerContext.reader(), acceptDocs, parents, idReaderTypeCache);
            } else {
                return null;
            }
        }

        @Override
        public void contextRewrite(SearchContext searchContext) throws Exception {
            searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
            parents = CacheRecycler.popHashSet();
            ParentUidsCollector collector = new ParentUidsCollector(parents, context, parentType);
            searchContext.searcher().search(parentQuery, collector);
            parents = collector.collectedUids;
        }

        @Override
        public void contextClear() {
            if (parents != null) {
                CacheRecycler.pushHashSet(parents);
            }
            parents = null;
        }

        static class ChildrenDocSet extends MatchDocIdSet {

            final IndexReader reader;
            final THashSet<HashedBytesArray> parents;
            final IdReaderTypeCache idReaderTypeCache;

            ChildrenDocSet(IndexReader reader, @Nullable Bits acceptDocs, THashSet<HashedBytesArray> parents, IdReaderTypeCache idReaderTypeCache) {
                super(reader.maxDoc(), acceptDocs);
                this.reader = reader;
                this.parents = parents;
                this.idReaderTypeCache = idReaderTypeCache;
            }

            @Override
            protected boolean matchDoc(int doc) {
                return parents.contains(idReaderTypeCache.parentIdByDoc(doc));
            }

        }

        static class ParentUidsCollector extends NoopCollector {

            final THashSet<HashedBytesArray> collectedUids;
            final SearchContext context;
            final String parentType;

            IdReaderTypeCache typeCache;

            ParentUidsCollector(THashSet<HashedBytesArray> collectedUids, SearchContext context, String parentType) {
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

    }

    static class Bitset extends HasParentFilter {

        Map<Object, FixedBitSet> parentDocs;

        Bitset(Query query, String scope, String parentType, SearchContext context) {
            super(query, scope, parentType, context);
        }

        public DocIdSet getDocIdSet(AtomicReaderContext readerContext, Bits acceptDocs) throws IOException {
            if (parentDocs == null) {
                throw new ElasticSearchIllegalStateException("has_parent filter hasn't executed properly");
            }

            IdReaderTypeCache currentTypeCache = context.idCache().reader(readerContext.reader()).type(parentType);
            if (currentTypeCache == null) {
                return null;
            } else {
                return new ChildrenDocSet(readerContext.reader(), currentTypeCache, acceptDocs, parentDocs, context, parentType);
            }
        }

        @Override
        public void contextRewrite(SearchContext searchContext) throws Exception {
            searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
            ParentDocsCollector collector = new ParentDocsCollector();
            searchContext.searcher().search(parentQuery, collector);
            parentDocs = collector.segmentResults;
        }

        @Override
        public void contextClear() {
            parentDocs = null;
        }

        static class ChildrenDocSet extends MatchDocIdSet {

            final IdReaderTypeCache currentTypeCache;
            final AtomicReader currentReader;
            final Tuple<AtomicReader, IdReaderTypeCache>[] readersToTypeCache;
            final Map<Object, FixedBitSet> parentDocs;

            ChildrenDocSet(AtomicReader currentReader, IdReaderTypeCache currentTypeCache, @Nullable Bits acceptDocs,
                           Map<Object, FixedBitSet> parentDocs, SearchContext context, String parentType) {
                super(currentReader.maxDoc(), acceptDocs);
                this.currentTypeCache = currentTypeCache;
                this.currentReader = currentReader;
                this.parentDocs = parentDocs;
                this.readersToTypeCache = new Tuple[context.searcher().getIndexReader().leaves().size()];
                for (int i = 0; i < readersToTypeCache.length; i++) {
                    AtomicReader reader = context.searcher().getIndexReader().leaves().get(i).reader();
                    readersToTypeCache[i] = new Tuple<AtomicReader, IdReaderTypeCache>(reader, context.idCache().reader(reader).type(parentType));
                }
            }

            @Override
            protected boolean matchDoc(int doc) {
                if (doc == -1) {
                    return false;
                }

                HashedBytesArray parentId = currentTypeCache.parentIdByDoc(doc);
                if (parentId == null) {
                    return false;
                }

                for (Tuple<AtomicReader, IdReaderTypeCache> readerTypeCacheTuple : readersToTypeCache) {
                    int parentDocId = readerTypeCacheTuple.v2().docById(parentId);
                    if (parentDocId == -1) {
                        continue;
                    }

                    FixedBitSet currentParentDocs = parentDocs.get(readerTypeCacheTuple.v1().getCoreCacheKey());
                    if (currentParentDocs.get(parentDocId)) {
                        return true;
                    }
                }
                return false;
            }
        }

        static class ParentDocsCollector extends NoopCollector {

            final Map<Object, FixedBitSet> segmentResults = newHashMap();
            FixedBitSet current;

            public void collect(int doc) throws IOException {
                current.set(doc);
            }

            @Override
            public void setNextReader(AtomicReaderContext context) throws IOException {
                segmentResults.put(context.reader().getCoreCacheKey(), current = new FixedBitSet(context.reader().maxDoc()));
            }
        }
    }

}

