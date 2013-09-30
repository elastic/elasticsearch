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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * This filters just exist for wrapping parent child queries in the delete by query api.
 * Don't use this filter for other purposes.
 *
 * @elasticsearch.internal
 */
public class DeleteByQueryWrappingFilter extends Filter {

    private final Query query;

    private IndexSearcher searcher;
    private Weight weight;

    /** Constructs a filter which only matches documents matching
     * <code>query</code>.
     */
    public DeleteByQueryWrappingFilter(Query query) {
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
            searcher = searchContext.searcher();
            IndexReader indexReader = SearchContext.current().searcher().getIndexReader();
            IndexReader multiReader = null;
            try {
                if (!contains(indexReader, context)) {
                    multiReader = new MultiReader(new IndexReader[]{indexReader, context.reader()}, false);
                    searcher = new IndexSearcher(new MultiReader(indexReader, context.reader()));
                }
                weight = searcher.createNormalizedWeight(query);
            } finally {
                if (multiReader != null) {
                    multiReader.close();
                }
            }
        } else {
            IndexReader indexReader = searcher.getIndexReader();
            if (!contains(indexReader, context)) {
                IndexReader multiReader = new MultiReader(new IndexReader[]{indexReader, context.reader()}, false);
                try {
                    searcher = new IndexSearcher(multiReader);
                    weight = searcher.createNormalizedWeight(query);
                } finally {
                    multiReader.close();
                }
            }
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
        return "DeleteByQueryWrappingFilter(" + query + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DeleteByQueryWrappingFilter))
            return false;
        return this.query.equals(((DeleteByQueryWrappingFilter)o).query);
    }

    @Override
    public int hashCode() {
        return query.hashCode() ^ 0x823D64CA;
    }

    static boolean contains(IndexReader indexReader, AtomicReaderContext context) {
        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            if (context.reader().getCoreCacheKey().equals(atomicReaderContext.reader().getCoreCacheKey())) {
                return true;
            }
        }
        return false;
    }
}
