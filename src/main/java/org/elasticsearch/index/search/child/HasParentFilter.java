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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * A filter that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public abstract class HasParentFilter extends Filter implements ScopePhase.CollectorPhase {

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

        public boolean requiresProcessing() {
            return parents == null;
        }

        public Collector collector() {
            parents = CacheRecycler.popHashSet();
            return new ParentUidsCollector(parents, context, parentType);
        }

        public void processCollector(Collector collector) {
            parents = ((ParentUidsCollector) collector).collectedUids;
        }

        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            if (parents == null) {
                throw new ElasticSearchIllegalStateException("has_parent filter hasn't executed properly");
            }

            IdReaderTypeCache idReaderTypeCache = context.idCache().reader(reader).type(parentType);
            if (idReaderTypeCache != null) {
                return new ChildrenDocSet(reader, parents, idReaderTypeCache);
            } else {
                return null;
            }
        }

        public void clear() {
            if (parents != null) {
                CacheRecycler.pushHashSet(parents);
            }
            parents = null;
        }

        static class ChildrenDocSet extends GetDocSet {

            final IndexReader reader;
            final THashSet<HashedBytesArray> parents;
            final IdReaderTypeCache idReaderTypeCache;

            ChildrenDocSet(IndexReader reader, THashSet<HashedBytesArray> parents, IdReaderTypeCache idReaderTypeCache) {
                super(reader.maxDoc());
                this.reader = reader;
                this.parents = parents;
                this.idReaderTypeCache = idReaderTypeCache;
            }

            public boolean get(int doc) {
                return !reader.isDeleted(doc) && parents.contains(idReaderTypeCache.parentIdByDoc(doc));
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
                collectedUids.add(typeCache.idByDoc(doc));
            }

            public void setNextReader(IndexReader reader, int docBase) throws IOException {
                typeCache = context.idCache().reader(reader).type(parentType);
            }
        }

    }

    static class Bitset extends HasParentFilter {

        Map<Object, FixedBitSet> parentDocs;

        Bitset(Query query, String scope, String parentType, SearchContext context) {
            super(query, scope, parentType, context);
        }

        public boolean requiresProcessing() {
            return parentDocs == null;
        }

        public Collector collector() {
            return new ParentDocsCollector();
        }

        public void processCollector(Collector collector) {
            parentDocs = ((ParentDocsCollector) collector).segmentResults;
        }

        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            if (parentDocs == null) {
                throw new ElasticSearchIllegalStateException("has_parent filter hasn't executed properly");
            }

            return new ChildrenDocSet(reader, parentDocs, context, parentType);
        }

        public void clear() {
            parentDocs = null;
        }

        static class ChildrenDocSet extends GetDocSet {

            final IdReaderTypeCache currentTypeCache;
            final IndexReader currentReader;
            final Tuple<IndexReader, IdReaderTypeCache>[] readersToTypeCache;
            final Map<Object, FixedBitSet> parentDocs;

            ChildrenDocSet(IndexReader currentReader, Map<Object, FixedBitSet> parentDocs,
                           SearchContext context, String parentType) {
                super(currentReader.maxDoc());
                this.currentTypeCache = context.idCache().reader(currentReader).type(parentType);
                this.currentReader = currentReader;
                this.parentDocs = parentDocs;
                this.readersToTypeCache = new Tuple[context.searcher().subReaders().length];
                for (int i = 0; i < readersToTypeCache.length; i++) {
                    IndexReader reader = context.searcher().subReaders()[i];
                    readersToTypeCache[i] = new Tuple<IndexReader, IdReaderTypeCache>(reader, context.idCache().reader(reader).type(parentType));
                }
            }

            public boolean get(int doc) {
                if (currentReader.isDeleted(doc) || doc == -1) {
                    return false;
                }

                HashedBytesArray parentId = currentTypeCache.parentIdByDoc(doc);
                if (parentId == null) {
                    return false;
                }

                for (Tuple<IndexReader, IdReaderTypeCache> readerTypeCacheTuple : readersToTypeCache) {
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

            public void setNextReader(IndexReader reader, int docBase) throws IOException {
                segmentResults.put(reader.getCoreCacheKey(), current = new FixedBitSet(reader.maxDoc()));
            }
        }
    }

}

