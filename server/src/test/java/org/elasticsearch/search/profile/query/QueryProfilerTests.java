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

package org.elasticsearch.search.profile.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.RandomApproximationQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class QueryProfilerTests extends ESTestCase {

    static Directory dir;
    static IndexReader reader;
    static ContextIndexSearcher searcher;

    @BeforeClass
    public static void setup() throws IOException {
        dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final int numDocs = TestUtil.nextInt(random(), 1, 20);
        for (int i = 0; i < numDocs; ++i) {
            final int numHoles = random().nextInt(5);
            for (int j = 0; j < numHoles; ++j) {
                w.addDocument(new Document());
            }
            Document doc = new Document();
            doc.add(new StringField("foo", "bar", Store.NO));
            w.addDocument(doc);
        }
        reader = w.getReader();
        w.close();
        searcher = new ContextIndexSearcher(reader, IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(), MAYBE_CACHE_POLICY);
    }

    @AfterClass
    public static void cleanup() throws IOException {
        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
        searcher = null;
    }

    public void testBasic() throws IOException {
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.search(query, 1);
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()).longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()).longValue(), equalTo(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count").longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count").longValue(), equalTo(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testNoScoring() throws IOException {
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.search(query, 1, Sort.INDEXORDER); // scores are not needed
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()).longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()).longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()).longValue(), equalTo(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count").longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count").longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count").longValue(), equalTo(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testUseIndexStats() throws IOException {
        QueryProfiler profiler = new QueryProfiler();
        searcher.setProfiler(profiler);
        Query query = new TermQuery(new Term("foo", "bar"));
        searcher.count(query); // will use index stats
        List<ProfileResult> results = profiler.getTree();
        assertEquals(0, results.size());

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));
    }

    public void testApproximations() throws IOException {
        QueryProfiler profiler = new QueryProfiler();
        // disable query caching since we want to test approximations, which won't
        // be exposed on a cached entry
        ContextIndexSearcher searcher = new ContextIndexSearcher(reader, IndexSearcher.getDefaultSimilarity(),
            null, MAYBE_CACHE_POLICY);
        searcher.setProfiler(profiler);
        Query query = new RandomApproximationQuery(new TermQuery(new Term("foo", "bar")), random());
        searcher.count(query);
        List<ProfileResult> results = profiler.getTree();
        assertEquals(1, results.size());
        Map<String, Long> breakdown = results.get(0).getTimeBreakdown();
        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString()).longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString()).longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString()).longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString()).longValue(), greaterThan(0L));

        assertThat(breakdown.get(QueryTimingType.CREATE_WEIGHT.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.BUILD_SCORER.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.NEXT_DOC.toString() + "_count").longValue(), greaterThan(0L));
        assertThat(breakdown.get(QueryTimingType.ADVANCE.toString() + "_count").longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.SCORE.toString() + "_count").longValue(), equalTo(0L));
        assertThat(breakdown.get(QueryTimingType.MATCH.toString() + "_count").longValue(), greaterThan(0L));

        long rewriteTime = profiler.getRewriteTime();
        assertThat(rewriteTime, greaterThan(0L));

    }

    public void testCollector() throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        ProfileCollector profileCollector = new ProfileCollector(collector);
        assertEquals(0, profileCollector.getTime());
        final LeafCollector leafCollector = profileCollector.getLeafCollector(reader.leaves().get(0));
        assertThat(profileCollector.getTime(), greaterThan(0L));
        long time = profileCollector.getTime();
        leafCollector.setScorer(null);
        assertThat(profileCollector.getTime(), greaterThan(time));
        time = profileCollector.getTime();
        leafCollector.collect(0);
        assertThat(profileCollector.getTime(), greaterThan(time));
    }

    private static class DummyQuery extends Query {

        @Override
        public String toString(String field) {
            return getClass().getSimpleName();
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
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
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {

                        @Override
                        public Scorer get(long loadCost) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long cost() {
                            return 42;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }
    }

    public void testScorerSupplier() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        w.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher s = newSearcher(reader);
        s.setQueryCache(null);
        Weight weight = s.createWeight(s.rewrite(new DummyQuery()), randomFrom(ScoreMode.values()), 1f);
        // exception when getting the scorer
        expectThrows(UnsupportedOperationException.class, () ->  weight.scorer(s.getIndexReader().leaves().get(0)));
        // no exception, means scorerSupplier is delegated
        weight.scorerSupplier(s.getIndexReader().leaves().get(0));
        reader.close();
        dir.close();
    }

    private static final QueryCachingPolicy ALWAYS_CACHE_POLICY = new QueryCachingPolicy() {

        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) throws IOException {
            return true;
        }

    };

    private static final QueryCachingPolicy NEVER_CACHE_POLICY = new QueryCachingPolicy() {

        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) throws IOException {
            return false;
        }

    };

}
