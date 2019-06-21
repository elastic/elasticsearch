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
import org.apache.lucene.search.similarities.AssertingSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.lucene.util.LuceneTestCase.random;

/**
 * Helper class that adds some extra checks to ensure correct
 * usage of {@code LeafIndexSearcher}.
 */
public class AssertingLeafIndexSearcher extends LeafIndexSearcher {
    public AssertingLeafIndexSearcher(IndexReader reader) {
        this(reader, IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy());
    }

    public AssertingLeafIndexSearcher(IndexReader reader, Similarity similarity) {
        this(reader, similarity, IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy());
    }

    public AssertingLeafIndexSearcher(IndexReader reader, Similarity similarity,
                                        QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        super(reader, similarity, queryCache, queryCachingPolicy);
    }

    @Override
    IndexSearcher newIndexSearcher() {
        AssertingIndexSearcher searcher = new AssertingIndexSearcher(random(), getIndexReader()) {
            @Override
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                for (LeafReaderContext ctx : leaves) {
                    searchLeaf(ctx, weight, collector);
                }
            }
        };
        searcher.setSimilarity(new AssertingSimilarity(similarity));
        if (queryCache != null) {
            searcher.setQueryCache(queryCache);
            searcher.setQueryCachingPolicy(queryCachingPolicy);
        }
        return searcher;
    }
}
