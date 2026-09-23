/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
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
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests recall aggregation and the ordered execution of baseline and candidate searches. */
public class TransportKnnEvalActionTests extends ESTestCase {

    private static final int K = 5;

    /** The stub's total hit count, which is what an exact baseline counts as its vector operations. */
    private static final long TOTAL_HITS = 30;

    /** A real {@link ClusterSettings}, since the action reading it is the point; only {@link ClusterService} is mocked. */
    private static ClusterService clusterService(boolean allowExpensiveQueries) {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(SearchService.ALLOW_EXPENSIVE_QUERIES.getKey(), allowExpensiveQueries).build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create("knn-eval-test-node"));
        return clusterService;
    }

    private static final float BASELINE_VISIT_PERCENTAGE = 100.0f;

    public void testRecallIsSetOverlapOverBaselineSize() {
        SearchHit[] baselineHits = searchHits("a", "b", "c", "d", "e");
        SearchHit[] candidateHits = searchHits("a", "b", "c", "x", "y");
        try {
            candidateHits[3].score(1);
            candidateHits[4].score(0);
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(0.6, detail.recall(), 0.0);
            assertEquals(0, detail.baselineMissedBetter());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallOfIdenticalRunsIsOne() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "c");
        try {
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallIgnoresOrderWithinTheWindow() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("c", "b", "a");
        try {
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testRecallOfDisjointRunsIsZero() {
        SearchHit[] baselineHits = searchHits("a", "b");
        SearchHit[] candidateHits = searchHits("x", "y");
        try {
            candidateHits[0].score(1);
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(0.0, detail.recall(), 0.0);
            assertEquals(0, detail.baselineMissedBetter());
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
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(1.0, detail.recall(), 0.0);
            assertEquals(0, detail.baselineMissedBetter());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testCandidateHitAboveBaselineCutoffInvalidatesRecall() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "x");
        candidateHits[2].score(2);
        try {
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertNull(detail.recall());
            assertEquals(1, detail.baselineMissedBetter());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testCandidateHitTiedWithBaselineCutoffDoesNotInvalidateRecall() {
        SearchHit[] baselineHits = searchHits("a", "b", "c");
        SearchHit[] candidateHits = searchHits("a", "b", "x");
        try {
            KnnEvalRecall.RecallResult detail = recall(candidateHits, baselineHits);
            assertEquals(2.0 / 3.0, detail.recall(), 0.0);
            assertEquals(0, detail.baselineMissedBetter());
        } finally {
            releaseScratchHits(baselineHits);
            releaseScratchHits(candidateHits);
        }
    }

    public void testTopKExcludingDropsTheQueryDocumentAndTruncates() {
        SearchHit[] hits = searchHits("q", "a", "b", "c", "d", "e");
        try {
            // dropping the sampled document from the k + 1 requested hits still leaves a full window of k
            assertEquals(List.of("a", "b", "c", "d", "e"), hitIds(KnnEvalRecall.topKExcluding(hits, "index/q", 5)));
            // ... and with nothing excluded the extra hit is truncated
            assertEquals(List.of("q", "a", "b", "c", "d"), hitIds(KnnEvalRecall.topKExcluding(hits, null, 5)));
            // exclusion can also happen part way down the list
            assertEquals(List.of("q", "a", "b", "d", "e"), hitIds(KnnEvalRecall.topKExcluding(hits, "index/c", 5)));
            // a shorter list than k comes back unchanged
            assertEquals(List.of("q", "a", "b", "c", "d", "e"), hitIds(KnnEvalRecall.topKExcluding(hits, null, 10)));
        } finally {
            releaseScratchHits(hits);
        }
    }

    public void testTopKExcludingUsesIndexQualifiedDocumentKey() {
        SearchHit[] hits = new SearchHit[] {
            searchHit(0, "index-a", "q", 3),
            searchHit(1, "index-b", "q", 2),
            searchHit(2, "index-a", "a", 1) };
        try {
            SearchHit[] kept = KnnEvalRecall.topKExcluding(hits, "index-a/q", 2);
            assertEquals(List.of("index-b/q", "index-a/a"), Arrays.stream(kept).map(KnnEvalRecall::key).toList());
        } finally {
            releaseScratchHits(hits);
        }
    }

    public void testSampledQueryIdsUseIndexQualifiedDocumentKeys() {
        SearchHit[] hits = new SearchHit[] { searchHit(0, "index-a", "q", 2), searchHit(1, "index-b", "q", 1) };
        for (SearchHit hit : hits) {
            hit.setDocumentField(new DocumentField("emb", List.of(1.0f, 2.0f)));
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 2);
        SearchResponse searchResponse = SearchResponseUtils.successfulResponse(searchHits);
        searchHits.decRef();
        try {
            assertEquals(
                List.of("index-a/q", "index-b/q"),
                KnnEvalSearches.extractSampledQueries(searchResponse, "emb").stream().map(KnnEvalQuery::getId).toList()
            );
        } finally {
            searchResponse.decRef();
        }
    }

    public void testSampleRequestOnlyMatchesDocumentsWithTheVectorField() {
        KnnEvalSample sample = new KnnEvalSample(10, 42);
        KnnEvalSpec spec = new KnnEvalSpec(
            "emb",
            K,
            null,
            sample,
            new KnnEvalSettings(100.0f, null, null, false),
            List.of(new KnnEvalSettings(5.0f, null, null, false))
        );

        SearchRequest request = KnnEvalSearches.buildSampleRequest(spec, sample, new BytesArray("test-pit"));

        assertThat(request.source().query(), instanceOf(FunctionScoreQueryBuilder.class));
        assertEquals(QueryBuilders.existsQuery("emb"), ((FunctionScoreQueryBuilder) request.source().query()).query());
        assertEquals(sample.getSize(), request.source().size());
    }

    /** A shard dropped from a pass would change the corpus the recall number describes, so partial results are refused. */
    public void testEverySearchRefusesPartialResults() {
        KnnEvalSample sample = new KnnEvalSample(10, 42);
        KnnEvalSpec approximate = new KnnEvalSpec(
            "emb",
            K,
            null,
            sample,
            new KnnEvalSettings(100.0f, null, null, false),
            List.of(new KnnEvalSettings(5.0f, null, null, false))
        );
        KnnEvalQuery query = new KnnEvalQuery("q0", VectorData.fromFloats(new float[] { 0 }));
        BytesReference pit = new BytesArray("test-pit");

        assertFalse(KnnEvalSearches.buildSampleRequest(approximate, sample, pit).allowPartialSearchResults());
        assertFalse(KnnEvalSearches.buildVectorCountRequest(approximate, pit).allowPartialSearchResults());
        assertFalse(KnnEvalSearches.buildSearch(approximate, query, approximate.getBaseline(), K, pit).allowPartialSearchResults());
        KnnEvalSpec exact = specWithBaseline(new KnnEvalSettings(null, null, null, true));
        assertFalse(KnnEvalSearches.buildSearch(exact, query, exact.getBaseline(), K, pit).allowPartialSearchResults());
    }

    public void testVectorCountRequestUsesThePointInTime() {
        KnnEvalSpec spec = specWithBaseline(new KnnEvalSettings(null, null, null, true));

        SearchRequest request = KnnEvalSearches.buildVectorCountRequest(spec, new BytesArray("test-pit"));

        assertEquals(QueryBuilders.existsQuery("emb"), request.source().query());
        assertEquals(0, request.source().size());
        assertEquals(Integer.MAX_VALUE, (int) request.source().trackTotalHitsUpTo());
        assertEquals(new PointInTimeBuilder(new BytesArray("test-pit")), request.source().pointInTimeBuilder());
    }

    public void testBaselinePassPrecedesOneHomogeneousPassPerCandidate() {
        RecordingClient client = new RecordingClient();
        run(client, 12, 5.0f, 20.0f);

        // every query of one settings entry runs before the next settings entry starts
        assertThat(client.passes(), contains(new Pass(BASELINE_VISIT_PERCENTAGE, 12), new Pass(5.0f, 12), new Pass(20.0f, 12)));
    }

    public void testCandidateMayUseTheSameSettingsAsTheBaseline() {
        RecordingClient client = new RecordingClient();
        KnnEvalResponse response = run(client, 10, BASELINE_VISIT_PERCENTAGE);

        assertEquals(1, response.getResults().size());
        assertEquals(10, client.candidateSearches);
    }

    public void testEveryQueryRunsOncePerPass() {
        RecordingClient client = new RecordingClient();
        run(client, 12, 5.0f, 20.0f);

        // one baseline pass plus one pass per settings entry
        assertEquals(12 * 3, client.searches.size());
    }

    /** The mean is over queries, not over passes. */
    public void testMeanRecallIsOverQueries() {
        // the stub's recall cycles 1.0, 0.8, 0.6, so the mean is (40 * 2.4) / 120
        assertEquals(0.8, runAndGetScore(120), 1e-9);
    }

    public void testQueriesWithBetterCandidateHitsAreExcludedFromMean() {
        RecordingClient client = new RecordingClient();
        client.candidateMissesAreBetter = true;

        KnnEvalResponse.KnnSettingsResult result = safeGet(execute(client, 3, null, 5.0f)).getResults().get(0);

        assertEquals(1.0, result.recall(), 0.0);
        assertEquals(1, result.includedQueries());
        assertEquals(2, result.excludedQueries());
    }

    public void testRecallIsNullWhenEveryQueryIsExcluded() {
        RecordingClient client = new RecordingClient();
        client.candidateMissesAreBetter = true;
        client.forceCandidateMiss = true;

        KnnEvalResponse.KnnSettingsResult result = safeGet(execute(client, 10, null, 5.0f)).getResults().get(0);

        assertNull(result.recall());
        assertEquals(0, result.includedQueries());
        assertEquals(10, result.excludedQueries());
    }

    public void testBaselineShortfallFailsQueriesInsteadOfComputingRecall() {
        RecordingClient client = new RecordingClient();
        client.baselineShortfall = true;

        KnnEvalResponse response = safeGet(execute(client, 3, null, 5.0f));
        KnnEvalResponse.KnnSettingsResult result = response.getResults().get(0);

        assertNull(result.recall());
        assertEquals(0, result.includedQueries());
        assertEquals(0, result.excludedQueries());
        assertEquals(3, response.getFailures().size());
        assertThat(response.getFailures().get("q0").getMessage(), containsString("fewer than [k=5]"));
        // no query has a reference result, so no candidate pass runs
        assertEquals(0, client.candidateSearches);
    }

    public void testExactBaselineIsGatedOnAllowExpensiveQueries() {
        KnnEvalSpec exactBaseline = specWithBaseline(new KnnEvalSettings(null, null, null, true));
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction blocked = new TransportKnnEvalAction(
            ActionFilters.EMPTY,
            new RecordingClient(),
            transportService,
            clusterService(false)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        blocked.doExecute(null, new KnnEvalRequest(exactBaseline, new String[] { "index" }), future);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(e.getMessage(), containsString("[exact] baseline requires [search.allow_expensive_queries] to be true"));

        // a non-exact baseline is an ordinary kNN search, so the setting does not apply
        RecordingClient client = new RecordingClient();
        TransportKnnEvalAction allowed = new TransportKnnEvalAction(ActionFilters.EMPTY, client, transportService, clusterService(false));
        PlainActionFuture<KnnEvalResponse> ok = new PlainActionFuture<>();
        allowed.doExecute(
            null,
            new KnnEvalRequest(specWithBaseline(new KnnEvalSettings(100.0f, null, null, false)), new String[] { "index" }),
            ok
        );
        assertEquals(1, safeGet(ok).getResults().size());
    }

    public void testExactBaselineWorkIsCappedByDocumentsTimesQueries() {
        KnnEvalSettings exact = new KnnEvalSettings(null, null, null, true);
        KnnEvalSettings candidate = new KnnEvalSettings(5.0f, null, null, false);
        KnnEvalSpec atLimit = new KnnEvalSpec("emb", K, null, new KnnEvalSample(10, null), exact, List.of(candidate));
        TransportKnnEvalAction.validateExactWorkload(atLimit, 10_000_000);

        KnnEvalSpec aboveLimit = new KnnEvalSpec("emb", K, null, new KnnEvalSample(11, null), exact, List.of(candidate));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportKnnEvalAction.validateExactWorkload(aboveLimit, 10_000_000)
        );
        assertThat(e.getMessage(), containsString("[110000000] full-precision vector comparisons"));
        assertThat(e.getMessage(), containsString("[100000000] limit"));
    }

    public void testCancelledTaskStopsBeforeStartingChildWork() {
        RecordingClient client = new RecordingClient();
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction action = new TransportKnnEvalAction(ActionFilters.EMPTY, client, transportService, clusterService(true));
        KnnEvalRequest request = new KnnEvalRequest(
            specWithBaseline(new KnnEvalSettings(20.0f, null, 100.0f, false)),
            new String[] { "index" }
        );
        CancellableTask task = (CancellableTask) request.createTask(
            1L,
            "transport",
            KnnEvalPlugin.KNN_EVAL_ACTION.name(),
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        TaskCancelHelper.cancel(task, "test");
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();

        action.doExecute(task, request, future);

        expectThrows(TaskCancelledException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertFalse(client.fieldMappingsRequested);
    }

    public void testSearchIsChildOfEvaluationTask() {
        RecordingClient client = new RecordingClient();
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        ClusterService clusterService = clusterService(true);
        TransportKnnEvalAction action = new TransportKnnEvalAction(ActionFilters.EMPTY, client, transportService, clusterService);
        KnnEvalRequest request = new KnnEvalRequest(
            specWithBaseline(new KnnEvalSettings(20.0f, null, null, false)),
            new String[] { "index" }
        );
        CancellableTask task = (CancellableTask) request.createTask(
            1L,
            "transport",
            KnnEvalPlugin.KNN_EVAL_ACTION.name(),
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();

        action.doExecute(task, request, future);

        assertNotNull(safeGet(future));
        assertEquals(new TaskId(clusterService.localNode().getId(), task.getId()), client.searchParentTask);
    }

    private static KnnEvalSpec specWithBaseline(KnnEvalSettings baseline) {
        return new KnnEvalSpec(
            "emb",
            K,
            List.of(new KnnEvalQuery("q0", VectorData.fromFloats(new float[] { 0 }))),
            null,
            baseline,
            List.of(new KnnEvalSettings(5.0f, null, null, false))
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
            new KnnEvalSettings(null, null, null, true),
            List.of(new KnnEvalSettings(5.0f, null, null, false))
        );
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction action = new TransportKnnEvalAction(
            ActionFilters.EMPTY,
            client,
            transportService,
            clusterService(allowExpensiveQueries)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        action.doExecute(null, new KnnEvalRequest(spec, new String[] { "index" }), future);

        assertTrue(client.vectorCountRequested);
        assertThat(client.baselineQuery, instanceOf(ExactKnnQueryBuilder.class));
        assertTrue("the exact baseline sets no knn section", client.baselineKnnSearchEmpty);
        // exact_knn is not profiled, so the scan's own hit count is the operation count
        assertFalse(client.baselineProfiled);
        KnnEvalResponse response = safeGet(future);
        assertEquals(KnnEvalResponse.FULL_PRECISION_SCAN, response.getBaselineVectorOpsKind());
        assertEquals(TOTAL_HITS, response.getBaselineVectorOps());
        // the settings entries are unaffected: they are what is being measured
        assertNull(client.candidateQuery);
        assertFalse(client.candidateKnnSearchEmpty);
    }

    /** The setting overrides the mapping's rescoring for that run. */
    public void testOversampleSettingSetsARescoreVectorBuilder() {
        RecordingClient withoutOversample = new RecordingClient();
        safeGet(execute(withoutOversample, 4, null, 5.0f));
        assertNull(withoutOversample.baselineRescoreVectorBuilder);

        RecordingClient withOversample = new RecordingClient();
        safeGet(execute(withOversample, 4, 10.0f, 5.0f));
        assertEquals(new RescoreVectorBuilder(10.0f), withOversample.baselineRescoreVectorBuilder);
        // only the run that set it is affected
        assertNull(withOversample.candidateRescoreVectorBuilder);
    }

    public void testFieldMustResolveIdenticallyAcrossIndices() {
        RecordingClient client = new RecordingClient();
        client.mismatchedFieldMappings = true;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> execute(client, 10, null, 5.0f).actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertThat(e.getMessage(), containsString("[emb] resolves differently across indices; evaluate one vector space at a time"));
        assertFalse(client.pointInTimeOpened);
    }

    public void testPointInTimeIsOpenedAndClosed() {
        RecordingClient client = new RecordingClient();
        run(client, 10, 5.0f);
        assertTrue("the vector field's similarity has to be resolved before any search runs", client.fieldMappingsRequested);
        assertTrue(client.pointInTimeOpened);
        assertTrue(client.pointInTimeClosed);
    }

    public void testRescoreCapIsReportedForExplicitAutoCalibratedOverride() {
        KnnEvalSpec calibratedSpec = specWithBaseline(new KnnEvalSettings(100.0f, null, 100.0f, false));
        KnnEvalResponse calibratedResponse = new KnnEvalState(
            calibratedSpec,
            false,
            calibratedSpec.getQueries(),
            new KnnEvalRescore(3.0f, true)
        ).buildResponse();
        assertFalse(calibratedResponse.getBaseline().rescoreWindowCapped());
        assertFalse(calibratedResponse.getResults().get(0).knnSettings().rescoreWindowCapped());

        KnnEvalSettings explicitCandidate = new KnnEvalSettings(5.0f, null, 10_000.0f, false);
        KnnEvalSpec explicitSpec = new KnnEvalSpec(
            "emb",
            K,
            calibratedSpec.getQueries(),
            null,
            calibratedSpec.getBaseline(),
            List.of(explicitCandidate)
        );
        KnnEvalResponse.ReportedSettings explicit = new KnnEvalState(
            explicitSpec,
            false,
            explicitSpec.getQueries(),
            new KnnEvalRescore(3.0f, true)
        ).buildResponse().getResults().get(0).knnSettings();
        assertTrue(explicit.rescoreWindowCapped());
    }

    /** The caller never invoked the mapping action, so the refusal has to name this endpoint and the privilege it needs. */
    public void testMappingLookupAuthorizationFailureNamesThisEndpoint() {
        RecordingClient client = new RecordingClient();
        client.failFieldMappings = true;
        ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            () -> execute(client, 10, null, 5.0f).actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertThat(exception.getMessage(), containsString("[_knn_eval] reads the mapping of field [emb]"));
        assertThat(exception.getMessage(), containsString("[view_index_metadata]"));
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        // the original refusal is still reachable for anyone debugging the privilege
        assertEquals("no view_index_metadata", exception.getCause().getMessage());
        assertFalse(client.pointInTimeOpened);
    }

    /** A non-authorization mapping failure is passed through untouched. */
    public void testNonAuthorizationMappingFailureIsPreserved() {
        RecordingClient client = new RecordingClient();
        client.failFieldMappings = true;
        client.fieldMappingsFailure = new ElasticsearchException("mapping lookup blew up");
        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> execute(client, 10, null, 5.0f).actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertEquals("mapping lookup blew up", exception.getMessage());
        assertFalse(client.pointInTimeOpened);
    }

    /** A failed search is attributed to its own query, and the point-in-time is still released. */
    public void testFailedSearchesAreReportedPerQueryAndReleaseThePointInTime() {
        RecordingClient client = new RecordingClient();
        client.failSearch = true;
        KnnEvalResponse response = safeGet(execute(client, 10, null, 5.0f));

        assertEquals(10, response.getFailures().size());
        assertEquals("search rejected", response.getFailures().get("q0").getMessage());
        // no query has a reference result, so no candidate pass runs
        assertEquals(0, client.candidateSearches);
        assertTrue(client.pointInTimeOpened);
        assertTrue(client.pointInTimeClosed);
    }

    private double runAndGetScore(int numQueries) {
        KnnEvalResponse response = run(new RecordingClient(), numQueries, 5.0f);
        assertEquals(1, response.getResults().size());
        assertEquals(0, response.getFailures().size());
        KnnEvalResponse.KnnSettingsResult result = response.getResults().get(0);
        assertEquals(numQueries, result.includedQueries());
        return result.recall();
    }

    private KnnEvalResponse run(RecordingClient client, int numQueries, float... candidateVisitPercentages) {
        return safeGet(execute(client, numQueries, null, candidateVisitPercentages));
    }

    private PlainActionFuture<KnnEvalResponse> execute(
        RecordingClient client,
        int numQueries,
        @Nullable Float baselineOversample,
        float... candidateVisitPercentages
    ) {
        boolean allowExpensiveQueries = true;
        client.baselineSearchesRemaining = numQueries;
        List<KnnEvalQuery> queries = new ArrayList<>(numQueries);
        for (int q = 0; q < numQueries; q++) {
            // the stub reads the ordinal back out of the vector to identify a query in any pass
            queries.add(new KnnEvalQuery("q" + q, VectorData.fromFloats(new float[] { q })));
        }
        List<KnnEvalSettings> candidates = new ArrayList<>(candidateVisitPercentages.length);
        for (float visitPercentage : candidateVisitPercentages) {
            candidates.add(new KnnEvalSettings(visitPercentage, null, null, false));
        }
        KnnEvalSpec spec = new KnnEvalSpec(
            "emb",
            K,
            queries,
            null,
            new KnnEvalSettings(BASELINE_VISIT_PERCENTAGE, null, baselineOversample, false),
            candidates
        );
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportKnnEvalAction action = new TransportKnnEvalAction(
            ActionFilters.EMPTY,
            client,
            transportService,
            clusterService(allowExpensiveQueries)
        );
        PlainActionFuture<KnnEvalResponse> future = new PlainActionFuture<>();
        action.doExecute(null, new KnnEvalRequest(spec, new String[] { "index" }), future);
        // the stub answers inline, so the whole chain has already completed
        return future;
    }

    /** One run of consecutive searches sharing a settings entry: which settings entry, and how many queries it covered. */
    private record Pass(Float visitPercentage, int queries) {}

    /** Records generated requests and answers inline; request ordering is observable only at the client boundary. */
    private static class RecordingClient extends NodeClient {

        private static final BytesReference POINT_IN_TIME_ID = new BytesArray("knn-eval-test-pit");

        private final List<Float> searches = new ArrayList<>();
        private boolean exactBaseline = false;
        private QueryBuilder baselineQuery;
        private QueryBuilder candidateQuery;
        private boolean baselineKnnSearchEmpty;
        private boolean baselineProfiled;
        private boolean candidateKnnSearchEmpty;
        private RescoreVectorBuilder baselineRescoreVectorBuilder;
        private RescoreVectorBuilder candidateRescoreVectorBuilder;
        private boolean fieldMappingsRequested = false;
        private boolean vectorCountRequested = false;
        private boolean failFieldMappings = false;
        private Exception fieldMappingsFailure;
        private boolean mismatchedFieldMappings = false;
        private boolean pointInTimeOpened = false;
        private boolean pointInTimeClosed = false;
        private boolean failSearch = false;
        private boolean candidateMissesAreBetter = false;
        private boolean forceCandidateMiss = false;
        private boolean baselineShortfall = false;
        private TaskId searchParentTask = TaskId.EMPTY_TASK_ID;
        private int baselineSearchesRemaining = 1;
        private int candidateSearches;

        /** Consecutive searches sharing a settings entry, collapsed into one entry per pass. */
        private List<Pass> passes() {
            List<Pass> passes = new ArrayList<>();
            for (Float visitPercentage : searches) {
                if (passes.isEmpty() == false && Objects.equals(passes.getLast().visitPercentage(), visitPercentage)) {
                    Pass last = passes.removeLast();
                    passes.add(new Pass(visitPercentage, last.queries() + 1));
                } else {
                    passes.add(new Pass(visitPercentage, 1));
                }
            }
            return passes;
        }

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
                    listener.onFailure(
                        fieldMappingsFailure == null
                            ? new ElasticsearchSecurityException("no view_index_metadata", RestStatus.FORBIDDEN)
                            : fieldMappingsFailure
                    );
                    return;
                }
                BytesReference mapping = new BytesArray("""
                    {"emb":{"type":"dense_vector","similarity":"l2_norm","element_type":"float",
                    "index_options":{"type":"bbq_disk","rescore_vector":{"oversample":3.0}}}}""");
                Map<String, GetFieldMappingsResponse.FieldMappingMetadata> indexMapping = Map.of(
                    "emb",
                    new GetFieldMappingsResponse.FieldMappingMetadata("emb", mapping)
                );
                Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings;
                if (mismatchedFieldMappings) {
                    BytesReference otherMapping = new BytesArray("""
                        {"emb":{"type":"dense_vector","dims":2,"similarity":"l2_norm","element_type":"float",
                        "index_options":{"type":"bbq_disk","rescore_vector":{"oversample":3.0}}}}""");
                    mappings = Map.of(
                        "index",
                        indexMapping,
                        "other-index",
                        Map.of("emb", new GetFieldMappingsResponse.FieldMappingMetadata("emb", otherMapping))
                    );
                } else {
                    mappings = Map.of("index", indexMapping);
                }
                // The map constructor is package-private, so only this response envelope is mocked.
                GetFieldMappingsResponse response = mock(GetFieldMappingsResponse.class);
                when(response.mappings()).thenReturn(mappings);
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
            if (TransportSearchAction.TYPE.equals(action)) {
                search((SearchRequest) request, (ActionListener<SearchResponse>) listener);
                return;
            }
            throw new AssertionError("unexpected action [" + action.name() + "]");
        }

        /** Answers one evaluation search, recording which settings entry it belonged to. */
        private void evaluationSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
            searchParentTask = request.getParentTask();
            if (failSearch) {
                listener.onFailure(new ElasticsearchException("search rejected"));
                return;
            }
            // SearchRequest#validate rejects indices alongside a point-in-time
            assertEquals(0, request.indices().length);
            SearchSourceBuilder source = request.source();
            assertEquals(new PointInTimeBuilder(POINT_IN_TIME_ID), source.pointInTimeBuilder());
            Float visitPercentage = null;
            int queryOrdinal = 0;
            if (source.knnSearch().isEmpty()) {
                // an exact run has no knn section
                assertTrue(exactBaseline);
            } else {
                // the knn section rather than the knn query: only the dfs-phase path profiles vector_operations_count
                assertNull(source.query());
                assertEquals(1, source.knnSearch().size());
                assertEquals("emb", source.knnSearch().get(0).getField());
                assertTrue(source.profile());
                visitPercentage = source.knnSearch().get(0).getVisitPercentage();
                queryOrdinal = (int) source.knnSearch().get(0).getQueryVector().asFloatVector()[0];
            }

            boolean baseline = baselineSearchesRemaining-- > 0;
            searches.add(visitPercentage);
            if (baseline) {
                baselineQuery = source.query();
                baselineKnnSearchEmpty = source.knnSearch().isEmpty();
                baselineProfiled = source.profile();
                baselineRescoreVectorBuilder = knnRescoreVectorBuilder(source);
            } else {
                candidateSearches++;
                candidateQuery = source.query();
                candidateKnnSearchEmpty = source.knnSearch().isEmpty();
                candidateRescoreVectorBuilder = knnRescoreVectorBuilder(source);
            }

            // a baseline never has misses, exact or not
            int misses = baseline ? 0 : forceCandidateMiss ? 1 : queryOrdinal % 3;
            int hitCount = baseline && baselineShortfall ? K - 1 : K;
            ActionListener.respondAndRelease(listener, response(misses, baseline == false && candidateMissesAreBetter, hitCount));
        }

        private static RescoreVectorBuilder knnRescoreVectorBuilder(SearchSourceBuilder source) {
            return source.knnSearch().isEmpty() ? null : source.knnSearch().get(0).getRescoreVectorBuilder();
        }

        /** The last {@code misses} hits are renamed so they fall outside the baseline's top-k. */
        private static SearchResponse response(int misses, boolean missesAreBetter, int hitCount) {
            SearchHit[] hits = new SearchHit[hitCount];
            for (int i = 0; i < hitCount; i++) {
                hits[i] = SearchHit.unpooled(i, i < hitCount - misses ? "doc" + i : "miss" + i);
                hits[i].shard(new SearchShardTarget("node", new ShardId("index", "uuid", 0), null));
                hits[i].score(i >= hitCount - misses ? (missesAreBetter ? K + 1 : 0) : K - i);
            }
            SearchHits searchHits = new SearchHits(hits, new TotalHits(TOTAL_HITS, TotalHits.Relation.EQUAL_TO), hitCount);
            SearchResponse searchResponse = SearchResponseUtils.successfulResponse(searchHits);
            searchHits.decRef(); // the response holds the only remaining reference
            return searchResponse;
        }

        @Override
        public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
            if (request.source().size() == 0) {
                vectorCountRequested = true;
                assertEquals(new PointInTimeBuilder(POINT_IN_TIME_ID), request.source().pointInTimeBuilder());
                assertEquals(QueryBuilders.existsQuery("emb"), request.source().query());
                ActionListener.respondAndRelease(listener, response(0, false, 0));
                return;
            }
            if (request.source().query() instanceof FunctionScoreQueryBuilder) {
                throw new AssertionError("these tests supply queries explicitly, so no sampling search should be issued");
            }
            evaluationSearch(request, listener);
        }
    }

    private static KnnEvalRecall.RecallResult recall(SearchHit[] candidateHits, SearchHit[] baselineHits) {
        return KnnEvalRecall.recallOf(candidateHits, KnnEvalRecall.baselineOf(baselineHits), 0, 0);
    }

    private static SearchHit[] searchHits(String... ids) {
        SearchHit[] hits = new SearchHit[ids.length];
        for (int i = 0; i < ids.length; i++) {
            hits[i] = searchHit(i, "index", ids[i], ids.length - i);
        }
        return hits;
    }

    private static SearchHit searchHit(int docId, String index, String id, float score) {
        SearchHit hit = new SearchHit(docId, id);
        hit.shard(new SearchShardTarget("node", new ShardId(index, "uuid", 0), null));
        hit.score(score);
        return hit;
    }

    private static List<String> hitIds(SearchHit[] hits) {
        return Arrays.stream(hits).map(SearchHit::getId).toList();
    }

    private static void releaseScratchHits(SearchHit[] scratchHits) {
        for (SearchHit hit : scratchHits) {
            hit.decRef();
        }
    }
}
