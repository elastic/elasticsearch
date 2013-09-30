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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Forked from {@link QueryWrapperFilter} to make sure the weight is only created once.
 * This filter should never be cached! This filter only exists for internal usage.
 *
 * @elasticsearch.internal
 */
public class CustomQueryWrappingFilter extends Filter {

    private final Query query;

    private IndexSearcher searcher;
    private Weight weight;

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
    public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
        SearchContext searchContext = SearchContext.current();
        if (weight == null) {
            assert searcher == null;
            IndexSearcher searcher = searchContext.searcher();
            weight = searcher.createNormalizedWeight(query);
            this.searcher = searcher;
        } else {
            assert searcher == SearchContext.current().searcher();
        }

        return new DocIdSet() {
            @Override
            public DocIdSetIterator iterator() throws IOException {
                return weight.scorer(context, true, false, acceptDocs);
            }
            @Override
            public boolean isCacheable() { return false; }
        };
    }

    @Override
    public String toString() {
        return "CustomQueryWrappingFilter(" + query + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CustomQueryWrappingFilter))
            return false;
        return this.query.equals(((CustomQueryWrappingFilter)o).query);
    }

    @Override
    public int hashCode() {
        return query.hashCode() ^ 0x823D64C9;
    }
}
