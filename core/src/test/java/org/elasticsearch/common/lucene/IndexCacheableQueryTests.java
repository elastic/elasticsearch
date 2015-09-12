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

package org.elasticsearch.common.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

public class IndexCacheableQueryTests extends ESTestCase {

    static class DummyIndexCacheableQuery extends IndexCacheableQuery {
        @Override
        public String toString(String field) {
            return "DummyIndexCacheableQuery";
        }

        @Override
        public Weight doCreateWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
            return new Weight(this) {

                @Override
                public void extractTerms(Set<Term> terms) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public float getValueForNormalization() throws IOException {
                    return 0;
                }

                @Override
                public void normalize(float norm, float topLevelBoost) {
                }

                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    return null;
                }

            };
        }
    }

    public void testBasics() throws IOException {
        DummyIndexCacheableQuery query = new DummyIndexCacheableQuery();
        QueryUtils.check(query);

        Query rewritten = query.rewrite(new MultiReader(new IndexReader[0]));
        QueryUtils.check(rewritten);
        QueryUtils.checkUnequal(query, rewritten);

        Query rewritten2 = query.rewrite(new MultiReader(new IndexReader[0]));
        QueryUtils.check(rewritten2);
        QueryUtils.checkUnequal(rewritten, rewritten2);
    }

    public void testCache() throws IOException {
        Directory dir = newDirectory();
        LRUQueryCache cache = new LRUQueryCache(10000, Long.MAX_VALUE);
        QueryCachingPolicy policy = QueryCachingPolicy.ALWAYS_CACHE;
        RandomIndexWriter writer = new RandomIndexWriter(getRandom(), dir);
        for (int i = 0; i < 10; ++i) {
            writer.addDocument(new Document());
        }

        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newSearcher(reader);
        reader = searcher.getIndexReader(); // reader might be wrapped
        searcher.setQueryCache(cache);
        searcher.setQueryCachingPolicy(policy);

        assertEquals(0, cache.getCacheSize());
        DummyIndexCacheableQuery query = new DummyIndexCacheableQuery();
        searcher.count(query);
        int expectedCacheSize = reader.leaves().size();
        assertEquals(expectedCacheSize, cache.getCacheSize());
        searcher.count(query);
        assertEquals(expectedCacheSize, cache.getCacheSize());

        writer.addDocument(new Document());

        IndexReader reader2 = writer.getReader();
        searcher = newSearcher(reader2);
        reader2 = searcher.getIndexReader(); // reader might be wrapped
        searcher.setQueryCache(cache);
        searcher.setQueryCachingPolicy(policy);

        // since the query is only cacheable at the index level, it has to be recomputed on all leaves
        expectedCacheSize += reader2.leaves().size();
        searcher.count(query);
        assertEquals(expectedCacheSize, cache.getCacheSize());
        searcher.count(query);
        assertEquals(expectedCacheSize, cache.getCacheSize());

        reader.close();
        reader2.close();
        writer.close();
        assertEquals(0, cache.getCacheSize());
        dir.close();
    }

}
