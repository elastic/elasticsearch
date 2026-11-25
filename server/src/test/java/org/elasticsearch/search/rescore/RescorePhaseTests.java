/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RescorePhaseTests extends IndexShardTestCase {

    public void testRescorePhaseCancellation() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
                final int numDocs = scaledRandomIntBetween(100, 200);
                for (int i = 0; i < numDocs; ++i) {
                    Document doc = new Document();
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher s = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    new QueryCachingPolicy() {
                        @Override
                        public void onUse(Query query) {}

                        @Override
                        public boolean shouldCache(Query query) {
                            return false;
                        }
                    },
                    true
                );
                IndexShard shard = newShard(true);
                try (TestSearchContext context = new TestSearchContext(null, shard, s)) {
                    context.parsedQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));
                    SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
                    context.setTask(task);
                    SearchContext wrapped = new FilteredSearchContext(context) {
                        @Override
                        public boolean lowLevelCancellation() {
                            return true;
                        }

                        @Override
                        public FetchDocValuesContext docValuesContext() {
                            return context.docValuesContext();
                        }

                        @Override
                        public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
                            return context.docValuesContext(docValuesContext);
                        }

                        @Override
                        public FetchFieldsContext fetchFieldsContext() {
                            return context.fetchFieldsContext();
                        }

                        @Override
                        public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
                            return context.fetchFieldsContext(fetchFieldsContext);
                        }
                    };
                    try (wrapped) {
                        Runnable cancellationChecks = RescorePhase.getCancellationChecks(wrapped);
                        assertNotNull(cancellationChecks);
                        TaskCancelHelper.cancel(task, "test cancellation");
                        assertTrue(wrapped.isCancelled());
                        expectThrows(TaskCancelledException.class, cancellationChecks::run);
                        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(10);
                        rescoreContext.setQuery(new ParsedQuery(Queries.ALL_DOCS_INSTANCE));
                        rescoreContext.setCancellationChecker(cancellationChecks);
                        expectThrows(
                            TaskCancelledException.class,
                            () -> new QueryRescorer().rescore(
                                new TopDocs(
                                    new TotalHits(10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                    new ScoreDoc[] { new ScoreDoc(0, 1.0f) }
                                ),
                                context.searcher(),
                                rescoreContext
                            )
                        );
                    }
                }
                closeShards(shard);
            }
        }
    }

    /**
     * Each rescorer gets its own node in the rescore profile, in execution order. The queries the rescorers run stay in the profile of
     * the main query, after the main query itself, each tagged once with the position of the rescorer that ran it.
     */
    public void testProfileRescorers() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = newContextIndexSearcher(reader);
                Profilers profilers = new Profilers(searcher);
                TopDocs firstPass = firstPass(searcher, profilers);
                List<RescoreContext> rescorers = List.of(queryRescoreContext(1, "c"), queryRescoreContext(2, "b"));
                try (SearchContext context = new ProfilingSearchContext(searcher, profilers, rescorers)) {
                    context.queryResult().topDocs(new TopDocsAndMaxScore(firstPass, 1f), new DocValueFormat[0]);

                    RescorePhase.execute(context);

                    SearchProfileQueryPhaseResult result = profilers.buildQueryPhaseResults();
                    List<ProfileResult> mainQueries = result.getQueryProfileResults().get(0).getQueryResults();
                    assertThat(
                        mainQueries.stream().map(ProfileResult::getLuceneDescription).toList(),
                        equalTo(List.of("f:a", "f:c", "f:b"))
                    );
                    assertThat(mainQueries.get(0).getDebugInfo(), equalTo(Map.of()));
                    assertThat(mainQueries.get(1).getDebugInfo(), equalTo(Map.of("rescorer", 0)));
                    assertThat(mainQueries.get(2).getDebugInfo(), equalTo(Map.of("rescorer", 1)));

                    List<ProfileResult> rescoreResults = result.getRescoreProfileResults();
                    assertThat(rescoreResults, hasSize(2));
                    assertRescoreResult(rescoreResults.get(0), "f:c", 1);
                    assertRescoreResult(rescoreResults.get(1), "f:b", 2);
                }
            }
        }
    }

    /**
     * A rescorer can add its own information to its profile through {@link RescoreContext#profiler()}, which is only set while the
     * rescorer runs on a profiled request.
     */
    public void testRescorerAddsDebugInfo() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = newContextIndexSearcher(reader);
                boolean profile = randomBoolean();
                Profilers profilers = profile ? new Profilers(searcher) : null;
                TopDocs firstPass = profile
                    ? firstPass(searcher, profilers)
                    : new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, 1f) });
                Rescorer debugInfoRescorer = new Rescorer() {
                    @Override
                    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) {
                        if (profile) {
                            assertNotNull(rescoreContext.profiler());
                            rescoreContext.profiler().addDebugInfo("model_time_in_nanos", 42L);
                        } else {
                            assertNull(rescoreContext.profiler());
                        }
                        return topDocs;
                    }

                    @Override
                    public Explanation explain(
                        int topLevelDocId,
                        IndexSearcher searcher,
                        RescoreContext rescoreContext,
                        Explanation sourceExplanation
                    ) {
                        throw new UnsupportedOperationException();
                    }
                };
                RescoreContext rescoreContext = new RescoreContext(10, debugInfoRescorer);
                try (SearchContext context = new ProfilingSearchContext(searcher, profilers, List.of(rescoreContext))) {
                    context.queryResult().topDocs(new TopDocsAndMaxScore(firstPass, 1f), new DocValueFormat[0]);

                    RescorePhase.execute(context);

                    assertNull(rescoreContext.profiler());
                    if (profile) {
                        List<ProfileResult> rescoreResults = profilers.buildQueryPhaseResults().getRescoreProfileResults();
                        assertThat(rescoreResults, hasSize(1));
                        assertThat(rescoreResults.get(0).getDebugInfo().get("model_time_in_nanos"), equalTo(42L));
                        assertThat(rescoreResults.get(0).getDebugInfo().get("window_size"), equalTo(10));
                    }
                }
            }
        }
    }

    /**
     * A rescorer that times out is still reported, flagged as timed out.
     */
    public void testProfileRescorerTimeout() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = newContextIndexSearcher(reader);
                Profilers profilers = new Profilers(searcher);
                TopDocs firstPass = firstPass(searcher, profilers);
                class TimingOutRescorer implements Rescorer {
                    @Override
                    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
                        searcher.createWeight(new TermQuery(new Term("f", "c")), ScoreMode.COMPLETE, 1f);
                        ((ContextIndexSearcher) searcher).throwTimeExceededException();
                        throw new AssertionError("the search should have timed out");
                    }

                    @Override
                    public Explanation explain(
                        int topLevelDocId,
                        IndexSearcher searcher,
                        RescoreContext rescoreContext,
                        Explanation sourceExplanation
                    ) {
                        throw new UnsupportedOperationException();
                    }
                }
                List<RescoreContext> rescorers = List.of(new RescoreContext(10, new TimingOutRescorer()));
                try (SearchContext context = new ProfilingSearchContext(searcher, profilers, rescorers)) {
                    context.queryResult().topDocs(new TopDocsAndMaxScore(firstPass, 1f), new DocValueFormat[0]);

                    RescorePhase.execute(context);

                    assertTrue(context.queryResult().searchTimedOut());
                    assertNull(rescorers.get(0).profiler());
                    SearchProfileQueryPhaseResult result = profilers.buildQueryPhaseResults();
                    List<ProfileResult> mainQueries = result.getQueryProfileResults().get(0).getQueryResults();
                    assertThat(mainQueries.stream().map(ProfileResult::getLuceneDescription).toList(), equalTo(List.of("f:a", "f:c")));
                    assertThat(mainQueries.get(1).getDebugInfo(), equalTo(Map.of("rescorer", 0)));

                    List<ProfileResult> rescoreResults = result.getRescoreProfileResults();
                    assertThat(rescoreResults, hasSize(1));
                    ProfileResult rescoreResult = rescoreResults.get(0);
                    assertThat(rescoreResult.getQueryName(), equalTo("TimingOutRescorer"));
                    assertThat(rescoreResult.getLuceneDescription(), equalTo("rescore top documents"));
                    assertThat(rescoreResult.getDebugInfo(), equalTo(Map.of("window_size", 10, "timed_out", true)));
                    assertThat(rescoreResult.getProfiledChildren(), empty());
                }
            }
        }
    }

    private static void assertRescoreResult(ProfileResult rescoreResult, String description, int windowSize) {
        assertThat(rescoreResult.getQueryName(), equalTo("query"));
        assertThat(rescoreResult.getLuceneDescription(), equalTo(description));
        assertThat(rescoreResult.getTimeBreakdown(), equalTo(Map.of()));
        assertThat(rescoreResult.getDebugInfo(), equalTo(Map.of("window_size", windowSize)));
        assertThat(rescoreResult.getProfiledChildren(), empty());
    }

    /**
     * Indexes three docs in this order: {@code f:a}, {@code f:[a, c]} and {@code f:c}.
     */
    private void indexDocs(Directory dir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
            for (List<String> values : List.of(List.of("a"), List.of("a", "c"), List.of("c"))) {
                Document doc = new Document();
                for (String value : values) {
                    doc.add(new StringField("f", value, Field.Store.NO));
                }
                w.addDocument(doc);
            }
        }
    }

    /**
     * Profiles the main query {@code f:a} on the searcher and returns its top docs, the first two docs of the index. Like the query
     * phase, it records a collector result, which is required to build the profile of the main query.
     */
    private static TopDocs firstPass(ContextIndexSearcher searcher, Profilers profilers) throws IOException {
        searcher.createWeight(searcher.rewrite(new TermQuery(new Term("f", "a"))), ScoreMode.COMPLETE, 1f);
        profilers.getCurrentQueryProfiler().setCollectorResult(new CollectorResult("collector", "reason", 0L, List.of()));
        return new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, 1f), new ScoreDoc(1, 1f) });
    }

    private static QueryRescorer.QueryRescoreContext queryRescoreContext(int windowSize, String value) {
        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(windowSize);
        rescoreContext.setQuery(new ParsedQuery(new TermQuery(new Term("f", value))));
        rescoreContext.setName(QueryRescorerBuilder.NAME);
        return rescoreContext;
    }

    private static ContextIndexSearcher newContextIndexSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            new QueryCachingPolicy() {
                @Override
                public void onUse(Query query) {}

                @Override
                public boolean shouldCache(Query query) {
                    return false;
                }
            },
            true
        );
    }

    /**
     * {@link TestSearchContext} does not support rescorers nor profiling, this subclass adds both.
     */
    private static class ProfilingSearchContext extends TestSearchContext {
        private final Profilers profilers;
        private final List<RescoreContext> rescorers;

        ProfilingSearchContext(ContextIndexSearcher searcher, Profilers profilers, List<RescoreContext> rescorers) {
            super(null, null, searcher);
            this.profilers = profilers;
            this.rescorers = rescorers;
        }

        @Override
        public int size() {
            return 10;
        }

        @Override
        public List<RescoreContext> rescore() {
            return rescorers;
        }

        @Override
        public Profilers getProfilers() {
            return profilers;
        }
    }
}
