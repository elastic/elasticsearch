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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class HasChildFilter extends Filter implements SearchContext.Rewrite {

    final Query childQuery;
    final String parentType;
    final String childType;
    final Filter parentFilter;
    final SearchContext searchContext;
    final int shortCircuitParentDocSet;

    Filter shortCircuitFilter;
    int remaining;
    THashSet<HashedBytesArray> collectedUids;

    public HasChildFilter(Query childQuery, String parentType, String childType, Filter parentFilter, SearchContext searchContext, int shortCircuitParentDocSet) {
        this.parentFilter = parentFilter;
        this.searchContext = searchContext;
        this.parentType = parentType;
        this.childType = childType;
        this.childQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
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
        return true;
    }

    @Override
    public int hashCode() {
        int result = childQuery.hashCode();
        result = 31 * result + childType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(childQuery).append(')');
        return sb.toString();
    }

    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        if (collectedUids == null) {
            throw new ElasticSearchIllegalStateException("has_child filter hasn't executed properly");
        }
        if (remaining == 0) {
            return null;
        }
        if (shortCircuitFilter != null) {
            return shortCircuitFilter.getDocIdSet(context, acceptDocs);
        }

        DocIdSet parentDocIdSet = this.parentFilter.getDocIdSet(context, null);
        if (DocIdSets.isEmpty(parentDocIdSet)) {
            return null;
        }

        Bits parentsBits = DocIdSets.toSafeBits(context.reader(), parentDocIdSet);
        IdReaderTypeCache idReaderTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
        if (idReaderTypeCache != null) {
            return new ParentDocSet(context.reader(), parentsBits, collectedUids, idReaderTypeCache);
        } else {
            return null;
        }
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());
        collectedUids = searchContext.cacheRecycler().popHashSet();
        UidCollector collector = new UidCollector(parentType, searchContext, collectedUids);
        searchContext.searcher().search(childQuery, collector);
        remaining = collectedUids.size();
        if (remaining == 0) {
            shortCircuitFilter = Queries.MATCH_NO_FILTER;
        } else if (remaining == 1) {
            BytesRef id = collectedUids.iterator().next().toBytesRef();
            shortCircuitFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
        } else if (remaining <= shortCircuitParentDocSet) {
            shortCircuitFilter = new ParentIdsFilter(parentType, collectedUids);
        }
    }

    @Override
    public void contextClear() {
        if (collectedUids != null) {
            searchContext.cacheRecycler().pushHashSet(collectedUids);
        }
        collectedUids = null;
        shortCircuitFilter = null;
    }

    final class ParentDocSet extends MatchDocIdSet {

        final IndexReader reader;
        final THashSet<HashedBytesArray> parents;
        final IdReaderTypeCache typeCache;

        ParentDocSet(IndexReader reader, Bits acceptDocs, THashSet<HashedBytesArray> parents, IdReaderTypeCache typeCache) {
            super(reader.maxDoc(), acceptDocs);
            this.reader = reader;
            this.parents = parents;
            this.typeCache = typeCache;
        }

        @Override
        protected boolean matchDoc(int doc) {
            if (remaining == 0) {
                shortCircuit();
                return false;
            }

            boolean match = parents.contains(typeCache.idByDoc(doc));
            if (match) {
                remaining--;
            }
            return match;
        }
    }

    final static class UidCollector extends ParentIdCollector {

        final THashSet<HashedBytesArray> collectedUids;

        UidCollector(String parentType, SearchContext context, THashSet<HashedBytesArray> collectedUids) {
            super(parentType, context);
            this.collectedUids = collectedUids;
        }

        @Override
        public void collect(int doc, HashedBytesArray parentIdByDoc) {
            collectedUids.add(parentIdByDoc);
        }

    }

}
