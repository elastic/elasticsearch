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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

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
     * Each rescorer gets its own node in the rescore profile, in execution order, with the queries it ran as children. Those queries
     * must not leak into the profile of the main query.
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
                    assertThat(mainQueries.stream().map(ProfileResult::getLuceneDescription).toList(), equalTo(List.of("f:a")));

                    List<ProfileResult> rescoreResults = result.getRescoreProfileResults();
                    assertThat(rescoreResults, hasSize(2));
                    assertRescoreResult(rescoreResults.get(0), 1, "f:c");
                    assertRescoreResult(rescoreResults.get(1), 2, "f:b");
                }
            }
        }
    }

    /**
     * A rescorer that times out is still reported, flagged as timed out, and the profiler of the main query is reinstalled on the
     * searcher afterwards.
     */
    public void testProfileRescorerTimeout() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = newContextIndexSearcher(reader);
                Profilers profilers = new Profilers(searcher);
                TopDocs firstPass = firstPass(searcher, profilers);
                Rescorer timingOutRescorer = new Rescorer() {
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
                };
                List<RescoreContext> rescorers = List.of(new RescoreContext(10, timingOutRescorer));
                try (SearchContext context = new ProfilingSearchContext(searcher, profilers, rescorers)) {
                    context.queryResult().topDocs(new TopDocsAndMaxScore(firstPass, 1f), new DocValueFormat[0]);

                    RescorePhase.execute(context);

                    assertTrue(context.queryResult().searchTimedOut());
                    searcher.createWeight(new TermQuery(new Term("f", "b")), ScoreMode.COMPLETE, 1f);
                    SearchProfileQueryPhaseResult result = profilers.buildQueryPhaseResults();
                    List<ProfileResult> mainQueries = result.getQueryProfileResults().get(0).getQueryResults();
                    assertThat(mainQueries.stream().map(ProfileResult::getLuceneDescription).toList(), equalTo(List.of("f:a", "f:b")));

                    List<ProfileResult> rescoreResults = result.getRescoreProfileResults();
                    assertThat(rescoreResults, hasSize(1));
                    ProfileResult rescoreResult = rescoreResults.get(0);
                    assertThat(rescoreResult.getQueryName(), equalTo(timingOutRescorer.getClass().getName()));
                    assertThat(rescoreResult.getDebugInfo().get("timed_out"), equalTo(true));
                    assertThat(rescoreResult.getDebugInfo(), not(hasKey("docs_after_rescore")));
                    assertThat(
                        rescoreResult.getProfiledChildren().stream().map(ProfileResult::getLuceneDescription).toList(),
                        equalTo(List.of("f:c"))
                    );
                }
            }
        }
    }

    private static void assertRescoreResult(ProfileResult rescoreResult, int windowSize, String rescoreQuery) {
        assertThat(rescoreResult.getQueryName(), equalTo("query"));
        assertThat(rescoreResult.getLuceneDescription(), equalTo("window_size=" + windowSize));
        assertThat(rescoreResult.getTimeBreakdown().get("rescore_count"), equalTo(1L));
        assertThat(rescoreResult.getDebugInfo().get("window_size"), equalTo(windowSize));
        assertThat(rescoreResult.getDebugInfo().get("docs_before_rescore"), equalTo(2));
        assertThat(rescoreResult.getDebugInfo().get("docs_after_rescore"), equalTo(2));
        assertThat(
            rescoreResult.getProfiledChildren().stream().map(ProfileResult::getLuceneDescription).toList(),
            equalTo(List.of(rescoreQuery))
        );
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
