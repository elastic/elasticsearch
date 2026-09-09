/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarity;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.rankeval.RankEvalMetricTestHelper.releaseScratchHits;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the derived-ratings arithmetic in isolation ({@link TransportKnnEvalAction#recallOf} turning two hit lists into a set overlap,
 * {@link TransportKnnEvalAction#topKExcluding} dropping a sampled query's own document first) and the execution shape: a baseline pass
 * followed by one homogeneous pass per candidate, every search pinned to one point-in-time, batched to bound the msearch fan-out.
 */
public class TransportKnnEvalActionTests extends ESTestCase {

    private static final int K = 5;

    /** The stub reports this as the total hit count, which is what an exact baseline counts as its vector operations. */
    private static final long TOTAL_HITS = 30;

    /**
     * A real {@link ClusterSettings} rather than a stub for the setting itself: the point of the test is that the action reads the
     * registered {@code search.allow_expensive_queries} setting, so the registry has to be the real one. Only the surrounding
     * {@link ClusterService} is mocked, since standing one up would need a node.
     */
    private static ClusterService clusterService(boolean allowExpensiveQueries) {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey(), allowExpensiveQueries).build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    private static final float BASELINE_VISIT_PERCENTAGE = 100.0f;

    public void testRecallIsSetOverlapOverBaselineSize() {
        SearchHit[] baselineHits = searchHits("a", "b", "c", "d", "e");
        SearchHit[] candidateHits = searchHits("a", "b", "c", "x", "y");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertEquals(0.6, detail.recall(), 0.0);
            assertEquals(3L, detail.relevantRetrieved());
            assertEquals(5L, detail.relevant());
            assertEquals(List.of("a", "b", "c", "x", "y"), ids(detail.hits()));
            // the ids the baseline returned carry its rank; the two the candidate invented carry none
            assertEquals(Arrays.asList(0, 1, 2, null, null), detail.hits().stream().map(KnnEvalResponse.RankedHit::baselineRank).toList());
            assertEquals(List.of("d", "e"), ids(detail.missed()));
            // a missed hit carries the score and rank the baseline gave it, so it needs no join against baseline_details
            assertEquals(Arrays.asList(3, 4), detail.missed().stream().map(KnnEvalResponse.RankedHit::baselineRank).toList());
            assertEquals(baselineHits[3].getScore(), detail.missed().get(0).score(), 0.0f);
            assertEquals(baselineHits[4].getScore(), detail.missed().get(1).score(), 0.0f);
            assertDetailInvariants(detail);
            assertValueRecallIsNotWorse(detail);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallOfIdenticalRunsIsOne() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "c");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
            assertEquals(List.of(), detail.missed());
            assertDetailInvariants(detail);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallIgnoresOrderWithinTheWindow() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("c", "b", "a");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
            // a reordered window is a perfect recall, but the ranks record that the candidate disagreed on the order
            assertEquals(Arrays.asList(2, 1, 0), detail.hits().stream().map(KnnEvalResponse.RankedHit::baselineRank).toList());
            assertDetailInvariants(detail);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallOfDisjointRunsIsZero() {
        SearchHit[] baselineHits = searchHits("a", "b");
        SearchHit[] candidateHits = searchHits("x", "y");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertEquals(0.0, detail.recall(), 0.0);
            assertEquals(0L, detail.relevantRetrieved());
            assertEquals(2L, detail.relevant());
            assertEquals(List.of("a", "b"), ids(detail.missed()));
            assertEquals(Arrays.asList(0, 1), detail.missed().stream().map(KnnEvalResponse.RankedHit::baselineRank).toList());
            assertDetailInvariants(detail);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    /** A shard that cannot return k documents should not look like a recall failure. */
    public void testRecallIsRelativeToWhatTheBaselineCouldReturn() {
        SearchHit[] baselineHits = searchHits("a", "b");
        SearchHit[] candidateHits = searchHits("a", "b");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
            assertEquals(2L, detail.relevant());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testEpsilonProfileIsZeroForIdenticalRuns() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "c");
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            assertFalse(detail.incomplete());
            // one entry per rank of k; the two ranks past the reference depth were never reachable
            assertEquals(Arrays.asList(0.0, 0.0, 0.0, null, null), detail.epsilonProfile());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testAShortCandidateRunIsIncomplete() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a");
        // searchHits scores by list length, so give the shared document the score the baseline gave it
        candidateHits[0].score(baselineHits[0].getScore());
        try {
            KnnEvalResponse.QueryDetail detail = recall(candidateHits, baselineHits);
            // the candidate has nothing at ranks 1 and 2, so its loss there is unbounded rather than zero
            assertTrue(detail.incomplete());
            assertEquals(Arrays.asList(0.0, null, null, null, null), detail.epsilonProfile());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testNoEpsilonProfileWhenFidelityIsSkipped() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "c");
        try {
            KnnEvalResponse.QueryDetail detail = recallWithoutFidelity(candidateHits, baselineHits);
            assertEquals(List.of(), detail.epsilonProfile());
            assertFalse(detail.incomplete());
            // the same guard covers value recall: without real similarities there is nothing to compare
            assertNull(detail.recallValue());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testTopKExcludingDropsTheQueryDocumentAndTruncates() {
        SearchHit[] hits = searchHits("q", "a", "b", "c", "d", "e");
        try {
            // the sampled document is its own nearest neighbour, so the k + 1 hits requested still leave a full window of k
            assertEquals(List.of("a", "b", "c", "d", "e"), hitIds(TransportKnnEvalAction.topKExcluding(hits, "q", 5)));
            // ... and when nothing is excluded the extra hit is simply truncated
            assertEquals(List.of("q", "a", "b", "c", "d"), hitIds(TransportKnnEvalAction.topKExcluding(hits, null, 5)));
            // exclusion can also happen part way down the list
            assertEquals(List.of("q", "a", "b", "d", "e"), hitIds(TransportKnnEvalAction.topKExcluding(hits, "c", 5)));
            // a shorter list than k comes back unchanged
            assertEquals(List.of("q", "a", "b", "c", "d", "e"), hitIds(TransportKnnEvalAction.topKExcluding(hits, null, 10)));
        } finally {
            releaseScratchHits(hits);
        }
    }

    public void testBaselinePassPrecedesOneHomogeneousPassPerCandidate() {
        RecordingClient client = new RecordingClient();
        run(client, 120, 50, 5.0f, 20.0f);

        // 120 queries in batches of 50 is three msearches per pass, and each msearch belongs to exactly one configuration
        assertThat(
            client.passes,
            contains(
                new Msearch(BASELINE_VISIT_PERCENTAGE, 50),
                new Msearch(BASELINE_VISIT_PERCENTAGE, 50),
                new Msearch(BASELINE_VISIT_PERCENTAGE, 20),
                new Msearch(5.0f, 50),
                new Msearch(5.0f, 50),
                new Msearch(5.0f, 20),
                new Msearch(20.0f, 50),
                new Msearch(20.0f, 50),
                new Msearch(20.0f, 20)
            )
        );
    }

    public void testQueriesAreSplitIntoSequentialBatches() {
        // one baseline pass plus one candidate pass, so twice the number of batches per pass
        assertEquals(2 * 3, runAndCountMsearches(120, 50));
        assertEquals(2 * 1, runAndCountMsearches(120, 120));
        assertEquals(2 * 1, runAndCountMsearches(120, 1000));
        assertEquals(2 * 120, runAndCountMsearches(120, 1));
    }

    /** Batching must not change the answer: the per-candidate mean is over queries, not over batches. */
    public void testBatchingDoesNotChangeTheAggregate() {
        // recall cycles 1.0, 0.8, 0.6 over the 120 queries, so the mean is (40 * 2.4) / 120
        double unbatched = runAndGetScore(120, 1000);
        assertEquals(0.8, unbatched, 1e-9);
        assertEquals(unbatched, runAndGetScore(120, 50), 0.0);
        assertEquals(unbatched, runAndGetScore(120, 7), 0.0);
        assertEquals(unbatched, runAndGetScore(120, 1), 0.0);
    }

    public void testExactBaselineIsGatedOnAllowExpensiveQueries() {
        KnnEvalSpec exactBaseline = specWithBaseline(new KnnEvalKnobs(null, null, null, true));
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction blocked = new TransportKnnEvalAction(
            mock(ActionFilters.class),
            new RecordingClient(),
            transportService,
            clusterService(false)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        blocked.doExecute(null, new KnnEvalRequest(exactBaseline, new String[] { "index" }), future);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("[exact] baseline requires [search.allow_expensive_queries] to be true"));

        // a non-exact baseline is an ordinary kNN search, so the setting does not apply to it
        RecordingClient client = new RecordingClient();
        TransportKnnEvalAction allowed = new TransportKnnEvalAction(
            mock(ActionFilters.class),
            client,
            transportService,
            clusterService(false)
        );
        PlainActionFuture<KnnEvalResponse> ok = new PlainActionFuture<>();
        allowed.doExecute(
            null,
            new KnnEvalRequest(specWithBaseline(new KnnEvalKnobs(100.0f, null, null, false)), new String[] { "index" }),
            ok
        );
        assertEquals(1, ok.actionGet().getResults().size());
    }

    private static KnnEvalSpec specWithBaseline(KnnEvalKnobs baseline) {
        return new KnnEvalSpec(
            "emb",
            K,
            List.of(new KnnEvalQuery("q0", VectorData.fromFloats(new float[] { 0 }))),
            null,
            baseline,
            List.of(new KnnEvalKnobs(5.0f, null, null, false)),
            false,
            null,
            50,
            1,
            false,
            null
        );
    }

    /** An exact baseline brute-forces every document, so it uses the exact_knn query rather than the approximate knn section. */
    public void testExactBaselineUsesTheExactKnnQuery() {
        boolean allowExpensiveQueries = true;
        RecordingClient client = new RecordingClient();
        client.exactBaseline = true;
        List<KnnEvalQuery> queries = List.of(new KnnEvalQuery("q0", VectorData.fromFloats(new float[] { 0 })));
        KnnEvalSpec spec = new KnnEvalSpec(
            "emb",
            K,
            queries,
            null,
            new KnnEvalKnobs(null, null, null, true),
            List.of(new KnnEvalKnobs(5.0f, null, null, false)),
            false,
            null,
            50,
            1,
            false,
            null
        );
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction action = new TransportKnnEvalAction(
            mock(ActionFilters.class),
            client,
            transportService,
            clusterService(allowExpensiveQueries)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        action.doExecute(null, new KnnEvalRequest(spec, new String[] { "index" }), future);

        assertThat(client.baselineQuery, instanceOf(ExactKnnQueryBuilder.class));
        assertTrue("the exact baseline sets no knn section", client.baselineKnnSearchEmpty);
        // no profile to read, so the scan's own hit count is the operation count
        assertFalse(client.baselineProfiled);
        KnnEvalResponse response = future.actionGet();
        assertEquals(KnnEvalResponse.FULL_PRECISION_SCAN, response.getBaselineVectorOpsKind());
        assertEquals(TOTAL_HITS, response.getBaselineVectorOps().sum());
        // candidates are unaffected: they are what is being measured
        assertNull(client.candidateQuery);
        assertFalse(client.candidateKnnSearchEmpty);
    }

    /** The knob overrides the mapping's rescoring for that run, which is how a baseline is made exact. */
    public void testOversampleKnobSetsARescoreVectorBuilder() {
        RecordingClient withoutKnob = new RecordingClient();
        execute(withoutKnob, 4, 4, true, null, 5.0f).actionGet();
        assertNull(withoutKnob.baselineRescoreVectorBuilder);

        RecordingClient withKnob = new RecordingClient();
        execute(withKnob, 4, 4, true, 10.0f, 5.0f).actionGet();
        assertEquals(new RescoreVectorBuilder(10.0f), withKnob.baselineRescoreVectorBuilder);
        // only the configuration that set it is affected
        assertNull(withKnob.candidateRescoreVectorBuilder);
    }

    public void testPointInTimeIsOpenedAndClosed() {
        RecordingClient client = new RecordingClient();
        run(client, 10, 5, 5.0f);
        assertTrue("the vector field's similarity has to be resolved before any search runs", client.fieldMappingsRequested);
        assertTrue(client.pointInTimeOpened);
        assertTrue(client.pointInTimeClosed);
    }

    /** The mapping is read for the derived knob fields either way, but the value-based metrics stay off unless asked for. */
    public void testNoValueMetricsWithoutTheFidelityFlag() {
        RecordingClient client = new RecordingClient();
        KnnEvalResponse response = execute(client, 10, 5, false, null, 5.0f).actionGet();
        assertTrue(client.fieldMappingsRequested);
        assertTrue(client.pointInTimeOpened);
        assertNull(response.getValueTolerance());
        assertNull(response.getResults().get(0).fidelity());
        assertNull(response.getResults().get(0).recallValue());
        // the fixture mapping is a plain l2_norm field with no rescoring, so nothing is rescored
        assertEquals(Integer.valueOf(0), response.getBaseline().rescoreWindow());
        assertEquals(Integer.valueOf(Math.round(1.5f * K)), response.getBaseline().effectiveNumCandidates());
    }

    /** Without the mapping there is nothing to resolve the knobs against, but the recall numbers are unaffected. */
    public void testDerivedKnobFieldsAreOmittedWhenTheMappingCannotBeRead() {
        RecordingClient client = new RecordingClient();
        client.failFieldMappings = true;
        KnnEvalResponse response = execute(client, 10, 5, false, null, 5.0f).actionGet();
        assertNull(response.getBaseline().rescoreWindow());
        assertNull(response.getBaseline().effectiveNumCandidates());
        // the stub's recall cycles 1.0, 0.8, 0.6 across queries, exactly as when the mapping is readable
        assertEquals(0.82, response.getResults().get(0).recall(), 1e-9);
    }

    public void testTheFidelityFlagTurnsTheValueMetricsOn() {
        RecordingClient client = new RecordingClient();
        KnnEvalResponse response = execute(client, 10, 5, true, null, 5.0f).actionGet();
        assertTrue(client.fieldMappingsRequested);
        assertEquals(Double.valueOf(0.0), response.getValueTolerance());
        assertNotNull(response.getResults().get(0).fidelity());
        assertNotNull(response.getResults().get(0).recallValue());
    }

    public void testPointInTimeIsClosedWhenTheEvaluationFails() {
        RecordingClient client = new RecordingClient();
        client.failMultiSearch = true;
        PlainActionFuture<KnnEvalResponse> future = execute(client, 10, 5, true, null, 5.0f);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertEquals("multi search rejected", e.getMessage());
        assertTrue(client.pointInTimeOpened);
        assertTrue(client.pointInTimeClosed);
    }

    private int runAndCountMsearches(int numQueries, int maxQueriesPerBatch) {
        RecordingClient client = new RecordingClient();
        run(client, numQueries, maxQueriesPerBatch, 5.0f);
        return client.passes.size();
    }

    private double runAndGetScore(int numQueries, int maxQueriesPerBatch) {
        KnnEvalResponse response = run(new RecordingClient(), numQueries, maxQueriesPerBatch, 5.0f);
        assertEquals(1, response.getResults().size());
        assertEquals(0, response.getFailures().size());
        KnnEvalResponse.CandidateResult result = response.getResults().get(0);
        assertEquals(result.recallStats().mean(), result.recall(), 0.0);
        assertEquals(result.recallValueStats().mean(), result.recallValue(), 0.0);
        assertTrue("value recall is never the lower of the two", result.recallValue() >= result.recall());
        assertEquals(result.tookMs().count(), result.recallStats().count());
        long histogramTotal = 0;
        double previous = Double.NEGATIVE_INFINITY;
        for (KnnEvalResponse.RecallBucket bucket : result.recallHistogram()) {
            assertTrue("buckets ascend by recall", bucket.recall() > previous);
            previous = bucket.recall();
            histogramTotal += bucket.count();
        }
        assertEquals("the histogram accounts for every query", result.recallStats().count(), histogramTotal);
        return result.recall();
    }

    private KnnEvalResponse run(RecordingClient client, int numQueries, int maxQueriesPerBatch, float... candidateVisitPercentages) {
        return execute(client, numQueries, maxQueriesPerBatch, true, null, candidateVisitPercentages).actionGet();
    }

    private PlainActionFuture<KnnEvalResponse> execute(
        RecordingClient client,
        int numQueries,
        int maxQueriesPerBatch,
        boolean includeFidelity,
        @Nullable Float baselineOversample,
        float... candidateVisitPercentages
    ) {
        boolean allowExpensiveQueries = true;
        List<KnnEvalQuery> queries = new ArrayList<>(numQueries);
        for (int q = 0; q < numQueries; q++) {
            // the stub reads the ordinal back out of the vector, so a query is identifiable whichever pass it turns up in
            queries.add(new KnnEvalQuery("q" + q, VectorData.fromFloats(new float[] { q })));
        }
        List<KnnEvalKnobs> candidates = new ArrayList<>(candidateVisitPercentages.length);
        for (float visitPercentage : candidateVisitPercentages) {
            candidates.add(new KnnEvalKnobs(visitPercentage, null, null, false));
        }
        KnnEvalSpec spec = new KnnEvalSpec(
            "emb",
            K,
            queries,
            null,
            new KnnEvalKnobs(BASELINE_VISIT_PERCENTAGE, null, baselineOversample, false),
            candidates,
            false,
            null,
            maxQueriesPerBatch,
            1,
            includeFidelity,
            null
        );
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction action = new TransportKnnEvalAction(
            mock(ActionFilters.class),
            client,
            transportService,
            clusterService(allowExpensiveQueries)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        action.doExecute(null, new KnnEvalRequest(spec, new String[] { "index" }), future);
        // the stub answers inline, so the whole chain of passes and batches has already completed by now
        return future;
    }

    /** One recorded msearch: which configuration it ran and how many queries it covered. */
    private record Msearch(Float visitPercentage, int queries) {}

    /**
     * A stub is unavoidable here: the behaviour under test is which requests the action issues and in what order, which is only
     * observable at the client boundary. Nothing lighter than a client can record that, and a real one would need a cluster.
     * <p>
     * It answers inline so that the recursive pass and batch chaining is exercised without a thread pool, and it fabricates hit lists
     * whose recall cycles 1.0, 0.8, 0.6 across queries so that the aggregate is sensitive to a folding bug (e.g. averaging batch means).
     */
    private static class RecordingClient extends NodeClient {

        private static final BytesReference POINT_IN_TIME_ID = new BytesArray("knn-eval-test-pit");

        private final List<Msearch> passes = new ArrayList<>();
        private boolean exactBaseline = false;
        private QueryBuilder baselineQuery;
        private QueryBuilder candidateQuery;
        private boolean baselineKnnSearchEmpty;
        private boolean baselineProfiled;
        private boolean candidateKnnSearchEmpty;
        private RescoreVectorBuilder baselineRescoreVectorBuilder;
        private RescoreVectorBuilder candidateRescoreVectorBuilder;
        private boolean fieldMappingsRequested = false;
        private boolean failFieldMappings = false;
        private boolean pointInTimeOpened = false;
        private boolean pointInTimeClosed = false;
        private boolean failMultiSearch = false;

        RecordingClient() {
            super(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(),
                null,
                TestProjectResolvers.alwaysThrow()
            );
        }

        @Override
        @SuppressWarnings("unchecked") // the responses below are the declared response type of the action they are matched against
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (GetFieldMappingsAction.INSTANCE.equals(action)) {
                fieldMappingsRequested = true;
                if (failFieldMappings) {
                    listener.onFailure(new ElasticsearchException("no view_index_metadata"));
                    return;
                }
                // GetFieldMappingsResponse has no public constructor, so a mock is the only way to hand one back from a stub client.
                // Only mappings() is consulted, and FieldMappingMetadata itself is real.
                BytesReference mapping = new BytesArray("""
                    {"emb":{"type":"dense_vector","similarity":"l2_norm","element_type":"float"}}""");
                GetFieldMappingsResponse response = mock(GetFieldMappingsResponse.class);
                when(response.mappings()).thenReturn(
                    Map.of("index", Map.of("emb", new GetFieldMappingsResponse.FieldMappingMetadata("emb", mapping)))
                );
                listener.onResponse((Response) response);
                return;
            }
            if (TransportOpenPointInTimeAction.TYPE.equals(action)) {
                pointInTimeOpened = true;
                listener.onResponse((Response) new OpenPointInTimeResponse(POINT_IN_TIME_ID, 1, 1, 0, 0, SearchResponse.Clusters.EMPTY));
                return;
            }
            if (TransportClosePointInTimeAction.TYPE.equals(action)) {
                assertEquals(POINT_IN_TIME_ID, ((ClosePointInTimeRequest) request).getId());
                pointInTimeClosed = true;
                listener.onResponse((Response) new ClosePointInTimeResponse(true, 1));
                return;
            }
            throw new AssertionError("unexpected action [" + action.name() + "]");
        }

        @Override
        public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
            if (failMultiSearch) {
                listener.onFailure(new ElasticsearchException("multi search rejected"));
                return;
            }
            Float visitPercentage = null;
            for (SearchRequest searchRequest : request.requests()) {
                // a point-in-time search resolves its own indices, and SearchRequest#validate rejects any being set alongside one
                assertEquals(0, searchRequest.indices().length);
                assertEquals(new PointInTimeBuilder(POINT_IN_TIME_ID), searchRequest.source().pointInTimeBuilder());
                if (searchRequest.source().knnSearch().isEmpty()) {
                    // an exact run has no knn section at all
                    assertTrue(exactBaseline);
                    continue;
                }
                // the top-level knn section, not the knn query: only the dfs-phase path profiles vector_operations_count
                assertNull(searchRequest.source().query());
                assertEquals(1, searchRequest.source().knnSearch().size());
                assertEquals("emb", searchRequest.source().knnSearch().get(0).getField());
                assertTrue(searchRequest.source().profile());
                Float searchVisitPercentage = searchRequest.source().knnSearch().get(0).getVisitPercentage();
                if (visitPercentage == null) {
                    visitPercentage = searchVisitPercentage;
                } else {
                    assertEquals("a batch must not mix configurations", visitPercentage, searchVisitPercentage);
                }
            }
            SearchSourceBuilder firstSource = request.requests().get(0).source();
            boolean baseline = firstSource.knnSearch().isEmpty() || BASELINE_VISIT_PERCENTAGE == visitPercentage;
            passes.add(new Msearch(visitPercentage, request.requests().size()));
            if (baseline) {
                baselineQuery = firstSource.query();
                baselineKnnSearchEmpty = firstSource.knnSearch().isEmpty();
                baselineProfiled = firstSource.profile();
                baselineRescoreVectorBuilder = knnRescoreVectorBuilder(firstSource);
            } else {
                candidateQuery = firstSource.query();
                candidateKnnSearchEmpty = firstSource.knnSearch().isEmpty();
                candidateRescoreVectorBuilder = knnRescoreVectorBuilder(firstSource);
            }

            MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
            for (int i = 0; i < items.length; i++) {
                // the ordinal is read back out of the knn section's vector; a baseline never has misses, exact or not
                SearchSourceBuilder source = request.requests().get(i).source();
                int misses = baseline ? 0 : (int) source.knnSearch().get(0).getQueryVector().asFloatVector()[0] % 3;
                items[i] = new MultiSearchResponse.Item(response(misses), null);
            }
            ActionListener.respondAndRelease(listener, new MultiSearchResponse(items, 1L));
        }

        private static RescoreVectorBuilder knnRescoreVectorBuilder(SearchSourceBuilder source) {
            return source.knnSearch().isEmpty() ? null : source.knnSearch().get(0).getRescoreVectorBuilder();
        }

        /** {@code K} hits, the last {@code misses} of which are renamed so that they fall outside the baseline's top-k. */
        private static SearchResponse response(int misses) {
            SearchHit[] hits = new SearchHit[K];
            for (int i = 0; i < K; i++) {
                hits[i] = SearchHit.unpooled(i, i < K - misses ? "doc" + i : "miss" + i);
                hits[i].shard(new SearchShardTarget("node", new ShardId("index", "uuid", 0), null));
                hits[i].score(K - i);
            }
            SearchHits searchHits = new SearchHits(hits, new TotalHits(TOTAL_HITS, TotalHits.Relation.EQUAL_TO), K);
            SearchResponse searchResponse = SearchResponseUtils.successfulResponse(searchHits);
            searchHits.decRef(); // the response holds the only remaining reference
            return searchResponse;
        }

        @Override
        public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
            throw new AssertionError("these tests supply queries explicitly, so no sampling search should be issued");
        }
    }

    /** Scores against a cosine field, whose score transform the scratch hits' scores are not meant to satisfy exactly. */
    private static KnnEvalResponse.QueryDetail recall(SearchHit[] candidateHits, SearchHit[] baselineHits) {
        return TransportKnnEvalAction.recallOf(
            "q1",
            candidateHits,
            TransportKnnEvalAction.baselineOf(baselineHits),
            new KnnEvalFidelity(VectorSimilarity.COSINE, null),
            5,
            0.0
        );
    }

    /** Fidelity is not computed at all when the field's scores are quantized estimates. */
    private static KnnEvalResponse.QueryDetail recallWithoutFidelity(SearchHit[] candidateHits, SearchHit[] baselineHits) {
        return TransportKnnEvalAction.recallOf(
            "q1",
            candidateHits,
            TransportKnnEvalAction.baselineOf(baselineHits),
            new KnnEvalFidelity(VectorSimilarity.COSINE, KnnEvalFidelity.RESCORING_DISABLED),
            5,
            0.0
        );
    }

    /** With no tolerance, an id match is always a value match too, so value recall can never be the lower of the two. */
    private static void assertValueRecallIsNotWorse(KnnEvalResponse.QueryDetail detail) {
        assertNotNull(detail.recallValue());
        assertTrue(
            "value recall [" + detail.recallValue() + "] is below id recall [" + detail.recall() + "]",
            detail.recallValue() >= detail.recall()
        );
    }

    /**
     * The two counts and the two lists have to agree, otherwise a caller reading {@code missed} and a caller reading
     * {@code relevant_retrieved} would draw different conclusions from the same response.
     */
    private static void assertDetailInvariants(KnnEvalResponse.QueryDetail detail) {
        long ranked = detail.hits().stream().filter(hit -> hit.baselineRank() != null).count();
        assertEquals("hits with a baseline rank are exactly the retrieved relevant ones", detail.relevantRetrieved(), ranked);
        assertEquals(
            "missed accounts for every relevant document not retrieved",
            detail.relevant() - detail.relevantRetrieved(),
            detail.missed().size()
        );
    }

    private static SearchHit[] searchHits(String... ids) {
        SearchHit[] hits = new SearchHit[ids.length];
        for (int i = 0; i < ids.length; i++) {
            hits[i] = new SearchHit(i, ids[i]);
            hits[i].shard(new SearchShardTarget("node", new ShardId("index", "uuid", 0), null));
            // descending scores, as a real search would return
            hits[i].score(ids.length - i);
        }
        return hits;
    }

    private static List<String> ids(List<KnnEvalResponse.RankedHit> hits) {
        return hits.stream().map(KnnEvalResponse.RankedHit::id).toList();
    }

    private static List<String> hitIds(SearchHit[] hits) {
        return Arrays.stream(hits).map(SearchHit::getId).toList();
    }
}
