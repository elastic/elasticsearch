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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.search.NoCacheFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * Forked from {@link QueryWrapperFilter} to make sure the weight is only created once.
 * This filter should never be cached! This filter only exists for internal usage.
 *
 * @elasticsearch.internal
 */
public class CustomQueryWrappingFilter extends NoCacheFilter implements Releasable {

    private final Query query;

    private IndexSearcher searcher;
    private IdentityHashMap<LeafReader, DocIdSet> docIdSets;

    /** Constructs a filter which only matches documents matching
     * <code>query</code>.
     */
    public CustomQueryWrappingFilter(Query query) {
        if (query == null)
            throw new NullPointerException("Query may not be null");
        this.query = query;
    }

    /** returns the inner Query */
    public final Query getQuery() {
        return query;
    }

    @Override
    public DocIdSet getDocIdSet(final LeafReaderContext context, final Bits acceptDocs) throws IOException {
        final SearchContext searchContext = SearchContext.current();
        if (docIdSets == null) {
            assert searcher == null;
            IndexSearcher searcher = searchContext.searcher();
            docIdSets = new IdentityHashMap<>();
            this.searcher = searcher;
            searchContext.addReleasable(this, Lifetime.COLLECTION);

            final Weight weight = searcher.createNormalizedWeight(query, false);
            for (final LeafReaderContext leaf : searcher.getTopReaderContext().leaves()) {
                final DocIdSet set = new DocIdSet() {
                    @Override
                    public DocIdSetIterator iterator() throws IOException {
                        return weight.scorer(leaf, null);
                    }
                    @Override
                    public boolean isCacheable() { return false; }

                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }
                };
                docIdSets.put(leaf.reader(), set);
            }
        } else {
            assert searcher == SearchContext.current().searcher();
        }
        final DocIdSet set = docIdSets.get(context.reader());
        return BitsFilteredDocIdSet.wrap(set, acceptDocs);
    }

    @Override
    public void close() throws ElasticsearchException {
        // We need to clear the docIdSets, otherwise this is leaved unused
        // DocIdSets around and can potentially become a memory leak.
        docIdSets = null;
        searcher = null;
    }

    @Override
    public String toString(String field) {
        return "CustomQueryWrappingFilter(" + query + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o != null && o instanceof CustomQueryWrappingFilter &&
                this.query.equals(((CustomQueryWrappingFilter)o).query)) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return query.hashCode() ^ 0x823D64C9;
    }

    /** @return Whether {@link CustomQueryWrappingFilter} should be used. */
    public static boolean shouldUseCustomQueryWrappingFilter(Query query) {
        if (query instanceof TopChildrenQuery || query instanceof ChildrenConstantScoreQuery
                || query instanceof ChildrenQuery || query instanceof ParentConstantScoreQuery
                || query instanceof ParentQuery) {
            return true;
        } else {
            return false;
        }
    }
}
