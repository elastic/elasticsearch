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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class HasChildFilter extends Filter implements ScopePhase.CollectorPhase {

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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        HasChildFilter that = (HasChildFilter) obj;
        if (!childQuery.equals(that.childQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = childQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + childType.hashCode();
        return result;
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

        public boolean requiresProcessing() {
            return parentDocs == null;
        }

        public Collector collector() {
            return new ChildCollector(parentType, searchContext);
        }

        public void processCollector(Collector collector) {
            this.parentDocs = ((ChildCollector) collector).parentDocs();
        }

        public void clear() {
            parentDocs = null;
        }

        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            if (parentDocs == null) {
                throw new ElasticSearchIllegalStateException("has_child filter hasn't executed properly");
            }

            // ok to return null
            return parentDocs.get(reader.getCoreCacheKey());
        }

    }

    static class Uid extends HasChildFilter {

        THashSet<HashedBytesArray> collectedUids;

        Uid(Query childQuery, String scope, String parentType, String childType, SearchContext searchContext) {
            super(childQuery, scope, parentType, childType, searchContext);
        }

        public boolean requiresProcessing() {
            return collectedUids == null;
        }

        public Collector collector() {
            collectedUids = CacheRecycler.popHashSet();
            return new UidCollector(parentType, searchContext, collectedUids);
        }

        public void processCollector(Collector collector) {
            collectedUids = ((UidCollector) collector).collectedUids;
        }

        public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
            if (collectedUids == null) {
                throw new ElasticSearchIllegalStateException("has_child filter hasn't executed properly");
            }

            IdReaderTypeCache idReaderTypeCache = searchContext.idCache().reader(reader).type(parentType);
            if (idReaderTypeCache != null) {
                return new ParentDocSet(reader, collectedUids, idReaderTypeCache);
            } else {
                return null;
            }
        }

        public void clear() {
            if (collectedUids != null) {
                CacheRecycler.pushHashSet(collectedUids);
            }
            collectedUids = null;
        }

        final static class ParentDocSet extends GetDocSet {

            final IndexReader reader;
            final THashSet<HashedBytesArray> parents;
            final IdReaderTypeCache typeCache;

            ParentDocSet(IndexReader reader, THashSet<HashedBytesArray> parents, IdReaderTypeCache typeCache) {
                super(reader.maxDoc());
                this.reader = reader;
                this.parents = parents;
                this.typeCache = typeCache;
            }

            public boolean get(int doc) {
                return !reader.isDeleted(doc) && parents.contains(typeCache.idByDoc(doc));
            }
        }

        final static class UidCollector extends ParentIdCollector {
            private final THashSet<HashedBytesArray> collectedUids;

            UidCollector(String parentType, SearchContext context, THashSet<HashedBytesArray> collectedUids) {
                super(parentType, context);
                this.collectedUids = collectedUids;
            }

            @Override
            public void collect(int doc, HashedBytesArray parentIdByDoc){
                collectedUids.add(parentIdByDoc);
            }
        }
    }
}
