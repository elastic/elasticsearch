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

package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Implements search over a single {@link LeafReaderContext}.
 */
public class LeafIndexSearcher {
    final IndexReader reader;
    private final IndexSearcher searcher;

    final Similarity similarity;
    final QueryCache queryCache;
    final QueryCachingPolicy queryCachingPolicy;

    /**
     * Return a new {@link LeafIndexSearcher} that copies the {@link IndexReader}, {@link Similarity},
     * {@link QueryCache} and {@link QueryCachingPolicy} from the provided <code>clone</code>.
     */
    public LeafIndexSearcher(LeafIndexSearcher clone) {
        this(clone.reader, clone.similarity, clone.queryCache, clone.queryCachingPolicy);
    }

    /**
     * Return a new {@link LeafIndexSearcher} that copies the {@link IndexReader} from the provided <code>clone</code>.
     */
    public LeafIndexSearcher(IndexReader reader, LeafIndexSearcher clone) {
        this(reader, clone.similarity, clone.queryCache, clone.queryCachingPolicy);
    }


    /**
     * Return a new {@link LeafIndexSearcher}
     */
    public LeafIndexSearcher(IndexReader reader, Similarity similarity, QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        this.reader = reader;
        this.similarity = Objects.requireNonNull(similarity);
        this.queryCache = queryCache;
        this.queryCachingPolicy = queryCache != null ? Objects.requireNonNull(queryCachingPolicy) : null;
        this.searcher = newIndexSearcher();
    }

    /**
     * Returns the top level reader
     */
    public IndexReader getIndexReader() {
        return reader;
    }

    /**
     * Returns the top level searcher
     */
    public IndexSearcher getIndexSearcher() {
        return searcher;
    }


    /**
     * Returns the {@link Similarity} associated with this searcher.
     */
    public Similarity getSimilarity() {
        return similarity;
    }

    /**
     * Returns the {@link QueryCache} associated with this searcher.
     * or null if the cache is disabled.
     */
    public QueryCache getQueryCache() {
        return queryCache;
    }

    /**
     * Returns the {@link QueryCachingPolicy} associated with this searcher
     * or null if the cache is disabled.
     */
    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /**
     * This method executes the searches on the given leaf exclusively.
     * To search across all the searchers leaves use {@link LeafIndexSearcher#getIndexSearcher()}
     * that will delegate the leaf search here.
     *
     * @param ctx
     *          the searcher leave to execute the search on
     * @param weight
     *          to match documents
     * @param collector
     *          to receive hits
     */
    public void searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector) throws IOException {
        assert ctx.parent.reader() == reader : "top level reader mismatch";
        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
        } catch (CollectionTerminatedException e) {
            // there is no doc of interest in this reader context
            // continue with the following leaf
            return;
        }
        BulkScorer scorer = weight.bulkScorer(ctx);
        if (scorer != null) {

            try {
                scorer.score(leafCollector, ctx.reader().getLiveDocs());
            } catch (CollectionTerminatedException e) {
                // collection was terminated prematurely
                // continue with the following leaf
            }
        }
    }

    // package protected in order to allow to change the IndexSearcher impl in tests (see AssertingLeafIndexSearcher)
    IndexSearcher newIndexSearcher() {
        IndexSearcher searcher = new IndexSearcher(reader) {
            @Override
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                for (LeafReaderContext ctx : leaves) {
                    searchLeaf(ctx, weight, collector);
                }
            }
        };
        searcher.setSimilarity(similarity);
        searcher.setQueryCache(queryCache);
        if (queryCache != null) {
            searcher.setQueryCachingPolicy(queryCachingPolicy);
        }
        return searcher;
    }
}
