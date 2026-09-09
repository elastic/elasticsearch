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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Estimates the recall of a cheap approximate kNN configuration without computing brute-force ground truth.
 * <p>
 * Ground truth for a large vector index is expensive enough that ANN recall usually goes unmeasured. This action substitutes a
 * <em>reference</em> run for ground truth: for every query vector it runs the caller's baseline knobs (typically a high
 * {@code visit_percentage}) and then each candidate, all against the same field, and reports how much of the baseline's top-k each
 * candidate recovered. Scoring reuses {@link RecallAtK}; the novelty is that the relevance judgements are <em>derived</em> rather than
 * supplied -- each document in the baseline's top-k becomes a {@link RatedDocument} with the relevant rating, which makes
 * {@code RecallAtK}'s output exactly the set overlap the caller wants.
 * <p>
 * <b>Execution shape.</b> Everything runs against a single point-in-time, so every configuration sees byte-for-byte the same segments;
 * without it a concurrent refresh or merge would show up as a recall difference. The passes are homogeneous: one pass of baseline
 * searches, then one pass per candidate, each pass batched to bound the msearch fan-out. Comparing like with like matters because a
 * mixed batch lets an expensive baseline search steal search threads from whichever candidate happens to be scheduled beside it.
 * <p>
 * <b>Reading {@code took_ms}.</b> It reflects the node's cache state at run time. The baseline pass runs first and is the most
 * exhaustive, so candidates execute against an index it has already warmed; their absolute latencies are therefore optimistic relative
 * to a cold cluster. With homogeneous passes a {@code max_concurrent_searches} above 1 still keeps candidates comparable <em>with each
 * other</em>, though it inflates every absolute value. There is deliberately no warm-up option: this API measures relative cost, and
 * {@code vector_ops} is the cache-independent axis to plot recall against.
 */
public class TransportKnnEvalAction extends HandledTransportAction<KnnEvalRequest, KnnEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKnnEvalAction.class);

    /** The rating given to every baseline hit. {@link RecallAtK}'s default threshold is also 1, so all baseline hits count as relevant. */
    private static final int RELEVANT_RATING = 1;

    /**
     * How long the point-in-time is held. It has to outlive the whole sweep -- every batch of every pass -- and is refreshed by nothing,
     * so it is generous relative to a single search.
     */
    static final TimeValue POINT_IN_TIME_KEEP_ALIVE = TimeValue.timeValueMinutes(5);

    /** Any positive value forces an exact query to score on the real vectors rather than the quantized ones. */
    private static final float EXACT_SCORING_OVERSAMPLE = 1.0f;

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportKnnEvalAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ClusterService clusterService
    ) {
        super(
            RankEvalPlugin.KNN_EVAL_ACTION.name(),
            transportService,
            actionFilters,
            KnnEvalRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, KnnEvalRequest request, ActionListener<KnnEvalResponse> listener) {
        if (request.getKnnEvalSpec().getBaseline().isExact()
            && clusterService.getClusterSettings().get(SearchService.ALLOW_EXPENSIVE_QUERIES) == false) {
            // An exact baseline scans every vector, which is exactly what that setting exists to keep off a cluster. A non-exact
            // baseline is an ordinary kNN search and is never gated here.
            listener.onFailure(
                new IllegalArgumentException(
                    "["
                        + KnnEvalKnobs.EXACT_FIELD.getPreferredName()
                        + "] baseline requires ["
                        + SearchService.ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "] to be true; set it or pass a non-exact baseline such as {visit_percentage: 100, oversample: 50}"
                )
            );
            return;
        }
        resolveField(
            request,
            listener.delegateFailureAndWrap((withField, fieldContext) -> openPointInTime(task, request, fieldContext, withField))
        );
    }

    /**
     * What the field's mapping tells us: how to invert a score into a similarity, and what a search will actually do with the knobs.
     * Either half is {@code null} when it could not be worked out.
     */
    record FieldContext(@Nullable KnnEvalFidelity fidelity, @Nullable KnnEvalRescore rescore) {
        static final FieldContext EMPTY = new FieldContext(null, null);
    }

    /**
     * Reads the vector field's mapping, which is what lets the response report the similarity-based metrics and the resolved candidate
     * windows.
     * <p>
     * The field mappings API is the least intrusive way to get at this from a transport action: it needs no extra injected service and
     * works whether or not the coordinating node holds a shard of the target indices. It does mean the caller needs
     * {@code view_index_metadata} on those indices in addition to read access. A caller without it should still get their recall, so a
     * failed lookup degrades to omitting the derived fields rather than failing the request.
     */
    private void resolveField(KnnEvalRequest request, ActionListener<FieldContext> listener) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        String field = spec.getField();
        GetFieldMappingsRequest mappingsRequest = new GetFieldMappingsRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .fields(field);
        client.execute(GetFieldMappingsAction.INSTANCE, mappingsRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetFieldMappingsResponse response) {
                Map<String, Object> fieldMapping = null;
                for (Map<String, FieldMappingMetadata> indexMappings : response.mappings().values()) {
                    FieldMappingMetadata metadata = indexMappings.get(field);
                    if (metadata != null && metadata.sourceAsMap().get(field) instanceof Map<?, ?> mapping) {
                        @SuppressWarnings("unchecked") // a field mapping body is always a string-keyed object
                        Map<String, Object> typed = (Map<String, Object>) mapping;
                        fieldMapping = typed;
                        break;
                    }
                }
                // a mapping that is present but wrong is a caller error worth reporting, unlike a mapping we simply could not read
                KnnEvalFidelity fidelity = spec.isIncludeFidelity()
                    ? KnnEvalFidelity.fromFieldMapping(field, fieldMapping, baselineOversample(spec.getBaseline()))
                    : null;
                listener.onResponse(
                    new FieldContext(fidelity, fieldMapping == null ? null : KnnEvalRescore.fromFieldMapping(fieldMapping))
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (spec.isIncludeFidelity()) {
                    // the value-based metrics were asked for by name and cannot be computed without the mapping
                    listener.onResponse(
                        new FieldContext(KnnEvalFidelity.unavailable("field mapping unavailable: " + e.getMessage()), null)
                    );
                    return;
                }
                logger.debug(() -> "could not read the mapping of field [" + field + "]; omitting the derived knob fields", e);
                listener.onResponse(FieldContext.EMPTY);
            }
        });
    }

    /** An exact baseline scores on the real vectors by construction, so it lifts the rescoring guard just as an explicit knob does. */
    @Nullable
    private static Float baselineOversample(KnnEvalKnobs baseline) {
        // boxed deliberately: a float branch would unbox the null one
        return baseline.isExact() ? Float.valueOf(EXACT_SCORING_OVERSAMPLE) : baseline.getOversample();
    }

    private void openPointInTime(Task task, KnnEvalRequest request, FieldContext fieldContext, ActionListener<KnnEvalResponse> listener) {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(POINT_IN_TIME_KEEP_ALIVE);
        client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, listener.delegateFailureAndWrap((delegate, openResponse) -> {
            BytesReference pointInTimeId = openResponse.getPointInTimeId();
            // runAfter fires on success and on failure alike, and ActionListener.run funnels anything thrown below into onFailure, so
            // there is no path out of here that leaves the point-in-time open.
            ActionListener<KnnEvalResponse> closingListener = ActionListener.runAfter(delegate, () -> closePointInTime(pointInTimeId));
            ActionListener.run(closingListener, l -> resolveQueries(task, request, fieldContext, pointInTimeId, l));
        }));
    }

    private void closePointInTime(BytesReference pointInTimeId) {
        client.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId), new ActionListener<>() {
            @Override
            public void onResponse(ClosePointInTimeResponse response) {}

            @Override
            public void onFailure(Exception e) {
                // The keep-alive expires on its own, so a failed close costs some search context memory but nothing correctness related.
                logger.warn("failed to close the point in time opened for kNN evaluation", e);
            }
        });
    }

    /** Either takes the caller's query vectors or samples them, in both cases handing off to the two-phase evaluation. */
    private void resolveQueries(
        Task task,
        KnnEvalRequest request,
        FieldContext fieldContext,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        KnnEvalSpec spec = request.getKnnEvalSpec();
        KnnEvalSample sample = spec.getSample();
        if (sample == null) {
            evaluate(task, spec, spec.getQueries(), false, fieldContext, pointInTimeId, listener);
            return;
        }
        client.search(buildSampleRequest(spec, sample, pointInTimeId), listener.delegateFailureAndWrap((delegate, searchResponse) -> {
            List<KnnEvalQuery> sampledQueries = extractSampledQueries(searchResponse, spec.getField());
            if (sampledQueries.isEmpty()) {
                // Reporting a recall of zero here would look like a catastrophic candidate rather than an empty index or a wrong
                // field name, so fail the request instead.
                throw new IllegalArgumentException(
                    "sampling query vectors from field ["
                        + spec.getField()
                        + "] returned no documents; check that the indices contain documents with that dense_vector field"
                );
            }
            evaluate(task, spec, sampledQueries, true, fieldContext, pointInTimeId, delegate);
        }));
    }

    private void evaluate(
        Task task,
        KnnEvalSpec spec,
        List<KnnEvalQuery> queries,
        boolean excludeQueryDocument,
        FieldContext fieldContext,
        BytesReference pointInTimeId,
        ActionListener<KnnEvalResponse> listener
    ) {
        runBaselineBatch(task, new KnnEvalState(spec, excludeQueryDocument, queries, fieldContext), pointInTimeId, 0, listener);
    }

    /**
     * Phase 1: the baseline pass. Runs only baseline searches, one batch at a time, recording each query's reference top-k before any
     * candidate runs.
     * <p>
     * Batching, not concurrency, is what bounds heap: a coordinator holds every sub-search response of one msearch until the last one
     * arrives, so a request covering thousands of queries exhausts the heap however few searches run at a time. The recursion happens
     * inside the msearch callback, which is safe because the state copies out everything it needs synchronously -- the previous batch's
     * response is released as that callback returns, and the next batch's callback runs on a fresh search-thread stack.
     */
    private void runBaselineBatch(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int from,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        List<KnnEvalQuery> queries = state.queries;
        if (from >= queries.size()) {
            runCandidatePass(task, state, pointInTimeId, 0, listener);
            return;
        }
        int to = Math.min(from + state.spec.getMaxQueriesPerBatch(), queries.size());
        List<KnnEvalQuery> batch = queries.subList(from, to);
        MultiSearchRequest msearchRequest = newMultiSearchRequest(state.spec);
        for (KnnEvalQuery query : batch) {
            msearchRequest.add(buildSearch(state.spec, query, state.spec.getBaseline(), state.searchSize, pointInTimeId));
        }
        client.multiSearch(msearchRequest, listener.delegateFailureAndWrap((delegate, msearchResponse) -> {
            state.addBaselineBatch(msearchResponse, batch);
            runBaselineBatch(task, state, pointInTimeId, to, delegate);
        }));
    }

    /** Phase 2: one homogeneous pass per candidate, in the order the caller listed them. */
    private void runCandidatePass(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int candidateIndex,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        if (candidateIndex >= state.spec.getCandidates().size()) {
            listener.onResponse(state.buildResponse());
            return;
        }
        runCandidateBatch(task, state, pointInTimeId, candidateIndex, 0, listener);
    }

    private void runCandidateBatch(
        Task task,
        KnnEvalState state,
        BytesReference pointInTimeId,
        int candidateIndex,
        int from,
        ActionListener<KnnEvalResponse> listener
    ) {
        if (checkCancelled(task, listener)) {
            return;
        }
        List<KnnEvalQuery> queries = state.evaluableQueries();
        if (from >= queries.size()) {
            runCandidatePass(task, state, pointInTimeId, candidateIndex + 1, listener);
            return;
        }
        int to = Math.min(from + state.spec.getMaxQueriesPerBatch(), queries.size());
        List<KnnEvalQuery> batch = queries.subList(from, to);
        KnnEvalKnobs candidate = state.spec.getCandidates().get(candidateIndex);
        MultiSearchRequest msearchRequest = newMultiSearchRequest(state.spec);
        for (KnnEvalQuery query : batch) {
            msearchRequest.add(buildSearch(state.spec, query, candidate, state.searchSize, pointInTimeId));
        }
        client.multiSearch(msearchRequest, listener.delegateFailureAndWrap((delegate, msearchResponse) -> {
            state.addCandidateBatch(candidateIndex, msearchResponse, batch);
            runCandidateBatch(task, state, pointInTimeId, candidateIndex, to, delegate);
        }));
    }

    private static boolean checkCancelled(Task task, ActionListener<KnnEvalResponse> listener) {
        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            listener.onFailure(new TaskCancelledException("task cancelled"));
            return true;
        }
        return false;
    }

    private static MultiSearchRequest newMultiSearchRequest(KnnEvalSpec spec) {
        MultiSearchRequest msearchRequest = new MultiSearchRequest();
        // Defaults to 1, which serializes the batch so that each search's reported took is its own shard time and not a figure
        // inflated by contention with its siblings.
        msearchRequest.maxConcurrentSearchRequests(spec.getMaxConcurrentSearches());
        return msearchRequest;
    }

    /**
     * Draws {@code sample.size} documents uniformly at random and fetches their vectors so they can be replayed as query vectors.
     * Sampling from the corpus itself keeps the query distribution matched to the indexed distribution, which is what makes the
     * resulting recall figure representative. It runs through the same point-in-time as the evaluation searches, so a sampled document
     * is guaranteed to be searchable in every pass.
     * <p>
     * {@link KnnEvalSpec#getFilter()} is deliberately not applied here: drawing queries from the filtered subset would make a
     * restrictive filter look harmless, when measuring recall under exactly that restriction is the point.
     */
    private static SearchRequest buildSampleRequest(KnnEvalSpec spec, KnnEvalSample sample, BytesReference pointInTimeId) {
        RandomScoreFunctionBuilder randomScore = new RandomScoreFunctionBuilder();
        if (sample.getSeed() != null) {
            // `field` is compulsory once a seed is set. `_seq_no` is unique per document within a shard, so it makes the draw
            // reproducible without every document collapsing onto the same random score.
            randomScore.seed(sample.getSeed()).setField(SeqNoFieldMapper.NAME);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), randomScore)
        ).size(sample.getSize()).fetchSource(false).fetchField(spec.getField()).pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return new SearchRequest().source(source);
    }

    /**
     * Copies the sampled vectors out of the search response. Everything is read eagerly because the response's pooled hits are released
     * as soon as this callback returns.
     */
    private static List<KnnEvalQuery> extractSampledQueries(SearchResponse searchResponse, String field) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<KnnEvalQuery> queries = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            float[] vector = extractVector(hit, field);
            if (vector != null) {
                queries.add(new KnnEvalQuery(hit.getId(), VectorData.fromFloats(vector)));
            }
        }
        return queries;
    }

    /**
     * @return the document's vector, or {@code null} if it has none (documents that never had a value for the field are simply not
     *         usable as queries and are skipped)
     */
    @Nullable
    private static float[] extractVector(SearchHit hit, String field) {
        DocumentField documentField = hit.field(field);
        if (documentField == null) {
            return null;
        }
        List<Object> values = documentField.getValues();
        if (values.isEmpty()) {
            return null;
        }
        float[] vector = new float[values.size()];
        for (int i = 0; i < vector.length; i++) {
            if (values.get(i) instanceof Number number) {
                vector[i] = number.floatValue();
            } else {
                throw new IllegalArgumentException(
                    "field [" + field + "] of document [" + hit.getId() + "] is not a numeric vector; is it a dense_vector field?"
                );
            }
        }
        return vector;
    }

    /**
     * One search of one configuration for one query. The request carries no indices and no indices options: a point-in-time search
     * resolves both from the point-in-time itself, and {@link SearchRequest#validate()} rejects either being set alongside one.
     */
    private static SearchRequest buildSearch(
        KnnEvalSpec spec,
        KnnEvalQuery query,
        KnnEvalKnobs knobs,
        int searchSize,
        BytesReference pointInTimeId
    ) {
        if (knobs.isExact()) {
            return new SearchRequest().source(
                // The profiler has no vector_operations_count for a query-phase exact_knn, but the work is known: one full-precision
                // comparison per document the query matched. An accurate total hit count is that number.
                new SearchSourceBuilder().query(exactQuery(spec, query))
                    .size(searchSize)
                    .fetchSource(false)
                    .trackTotalHitsUpTo(Integer.MAX_VALUE)
                    .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId))
            );
        }
        // num_candidates is validated against k, but the extra hit requested for sampled queries can push the window one past it.
        Integer numCandidates = knobs.getNumCandidates() == null ? null : Math.max(knobs.getNumCandidates(), searchSize);
        KnnSearchBuilder.Builder knnSearch = new KnnSearchBuilder.Builder().field(spec.getField())
            .queryVector(query.getQueryVector())
            .k(searchSize)
            .numCandidates(numCandidates)
            .visitPercentage(knobs.getVisitPercentage())
            // left null so that the field mapping's own rescoring applies unless this run overrides it
            .rescoreVectorBuilder(knobs.getOversample() == null ? null : new RescoreVectorBuilder(knobs.getOversample()));
        if (spec.getFilter() != null) {
            knnSearch.addFilterQueries(List.of(spec.getFilter()));
        }
        // The top-level knn section, not the equivalent knn query, because only the dfs-phase knn path records
        // vector_operations_count in its profile output -- and that count is the load-independent cost axis this API reports.
        // Builder.build(size) applies the same num_candidates default (1.5 * k) the query form would, so leaving it null is not a
        // behaviour change. profile(true) is on purely to harvest that count; everything else in the profile is discarded.
        SearchSourceBuilder source = new SearchSourceBuilder().knnSearch(List.of(knnSearch.build(searchSize)))
            .size(searchSize)
            .fetchSource(false)
            .profile(true)
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return new SearchRequest().source(source);
    }

    /**
     * Brute force over every document with a vector.
     * <p>
     * The oversample argument does not oversample anything here -- there is nothing to oversample when every document is scored -- it
     * selects the scoring fidelity, and any value above zero makes a quantized field score on its real vectors. Passing one explicitly
     * rather than {@code null} means an exact baseline is full precision even on a field whose mapping has rescoring switched off.
     * <p>
     * {@link ExactKnnQueryBuilder} carries no filter of its own, so a request filter becomes a bool clause around it.
     */
    private static QueryBuilder exactQuery(KnnEvalSpec spec, KnnEvalQuery query) {
        QueryBuilder exactKnn = new ExactKnnQueryBuilder(query.getQueryVector(), spec.getField(), null, EXACT_SCORING_OVERSAMPLE);
        if (spec.getFilter() == null) {
            return exactKnn;
        }
        return new BoolQueryBuilder().must(exactKnn).filter(spec.getFilter());
    }

    /**
     * A query's reference result, carried from the baseline pass into every candidate pass.
     *
     * @param hits      the baseline's top-k as {@code (_id, _score)} pairs, for the response details
     * @param ratedDocs the same documents as derived relevance judgements, which is what {@link RecallAtK} consumes. They are built in
     *                  the baseline pass while the live hits still expose their index name, since the overlap is keyed on
     *                  {@code _index}/{@code _id} and not on {@code _id} alone.
     */
    record BaselineResult(List<KnnEvalResponse.Hit> hits, List<RatedDocument> ratedDocs) {}

    /** Captures the reference result of one query from its live baseline hits, which do not outlive the msearch callback. */
    static BaselineResult baselineOf(SearchHit[] baselineHits) {
        List<RatedDocument> ratedDocs = new ArrayList<>(baselineHits.length);
        for (SearchHit hit : baselineHits) {
            ratedDocs.add(new RatedDocument(hit.getIndex(), hit.getId(), RELEVANT_RATING));
        }
        return new BaselineResult(toHits(baselineHits), ratedDocs);
    }

    /**
     * Scores one query's candidate run against the stored baseline by handing the derived ratings to {@link RecallAtK}, then annotates
     * the candidate's hits with where the baseline ranked each of them and lists the baseline documents it missed. Those two views are
     * what turn a bare recall number into something actionable, and computing them here means the response never has to repeat the
     * baseline hit list per candidate.
     * <p>
     * {@code relevant} is the size of the baseline hit list rather than {@code k}, so a shard that simply cannot return {@code k}
     * documents yields a recall of 1.0 rather than a misleading fraction.
     */
    static KnnEvalResponse.QueryDetail recallOf(
        String queryId,
        SearchHit[] candidateHits,
        BaselineResult baseline,
        @Nullable KnnEvalFidelity fidelity,
        int k,
        double valueTolerance
    ) {
        EvalQueryQuality quality = new RecallAtK().evaluate(queryId, candidateHits, baseline.ratedDocs());
        try {
            RecallAtK.Detail detail = (RecallAtK.Detail) quality.getMetricDetails();
            Map<String, Integer> baselineRanks = Maps.newMapWithExpectedSize(baseline.hits().size());
            for (int rank = 0; rank < baseline.hits().size(); rank++) {
                baselineRanks.putIfAbsent(baseline.hits().get(rank).id(), rank);
            }
            List<KnnEvalResponse.RankedHit> annotatedHits = new ArrayList<>(candidateHits.length);
            Set<String> returnedIds = Sets.newHashSetWithExpectedSize(candidateHits.length);
            for (SearchHit hit : candidateHits) {
                annotatedHits.add(new KnnEvalResponse.RankedHit(hit.getId(), hit.getScore(), baselineRanks.get(hit.getId())));
                returnedIds.add(hit.getId());
            }
            List<KnnEvalResponse.RankedHit> missed = new ArrayList<>();
            for (int rank = 0; rank < baseline.hits().size(); rank++) {
                KnnEvalResponse.Hit baselineHit = baseline.hits().get(rank);
                if (returnedIds.contains(baselineHit.id()) == false) {
                    missed.add(new KnnEvalResponse.RankedHit(baselineHit.id(), baselineHit.score(), rank));
                }
            }
            assert detail.getRelevant() - detail.getRelevantRetrieved() == missed.size()
                : "missed [" + missed.size() + "] does not account for " + detail.getRelevant() + " - " + detail.getRelevantRetrieved();

            // The two hit lists are each ordered by score, so rank i of one is comparable with rank i of the other without matching ids.
            List<Double> epsilonProfile = List.of();
            boolean incomplete = false;
            if (fidelity != null && fidelity.isSkipped() == false) {
                List<Double> profile = new ArrayList<>(k);
                for (int rank = 0; rank < k; rank++) {
                    if (rank >= baseline.hits().size()) {
                        // the reference never reached this rank either, so there is nothing to have lost
                        profile.add(null);
                    } else if (rank >= candidateHits.length) {
                        profile.add(null);
                        incomplete = true;
                    } else {
                        Double epsilon = fidelity.epsilonAtRank(baseline.hits().get(rank).score(), candidateHits[rank].getScore());
                        profile.add(epsilon);
                        incomplete |= epsilon == null;
                    }
                }
                epsilonProfile = profile;
            }
            // Value recall shares the id recall's denominator, so the two are directly comparable; the gap between them is the whole
            // point of having both.
            Double recallValue = null;
            long valueMatches = 0;
            if (fidelity != null && fidelity.isSkipped() == false && baseline.hits().isEmpty() == false) {
                float baselineWorstScore = baseline.hits().get(baseline.hits().size() - 1).score();
                for (SearchHit hit : candidateHits) {
                    if (fidelity.isValueMatch(baselineWorstScore, hit.getScore(), valueTolerance)) {
                        valueMatches++;
                    }
                }
                recallValue = (double) Math.min(valueMatches, baseline.hits().size()) / baseline.hits().size();
            }
            return new KnnEvalResponse.QueryDetail(
                quality.metricScore(),
                detail.getRelevantRetrieved(),
                detail.getRelevant(),
                annotatedHits,
                missed,
                epsilonProfile,
                incomplete,
                recallValue,
                valueMatches
            );
        } finally {
            // RecallAtK wraps every hit in a RatedSearchHit, which inc-refs it. The search response owns the hits themselves and
            // releases them when the msearch callback returns, so the extra refs taken here have to be dropped here too.
            for (RatedSearchHit ratedSearchHit : quality.getHitsAndRatings()) {
                ratedSearchHit.getSearchHit().decRef();
            }
        }
    }

    private static List<KnnEvalResponse.Hit> toHits(SearchHit[] hits) {
        List<KnnEvalResponse.Hit> result = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            result.add(new KnnEvalResponse.Hit(hit.getId(), hit.getScore()));
        }
        return result;
    }

    /**
     * The top {@code k} hits, skipping {@code excludedId}. Both runs are trimmed identically so that the overlap is computed over
     * comparable windows.
     */
    static SearchHit[] topKExcluding(SearchHit[] hits, @Nullable String excludedId, int k) {
        List<SearchHit> kept = new ArrayList<>(Math.min(hits.length, k));
        for (SearchHit hit : hits) {
            if (excludedId != null && excludedId.equals(hit.getId())) {
                continue;
            }
            kept.add(hit);
            if (kept.size() == k) {
                break;
            }
        }
        return kept.toArray(new SearchHit[0]);
    }

    /**
     * Everything one {@code _knn_eval} request accumulates, keyed by query id rather than by position: with a pass per configuration the
     * responses of a batch no longer interleave configurations, and a query dropped by a baseline failure must not shift the queries
     * after it.
     * <p>
     * The per-candidate mean cannot be computed batch by batch and then averaged, because batches differ in size and in how many of
     * their queries succeeded. The per-query values are therefore kept across batches and summarised once, in
     * {@link #buildResponse()}.
     */
    static class KnnEvalState {

        final KnnEvalSpec spec;
        final boolean excludeQueryDocument;
        final List<KnnEvalQuery> queries;
        /**
         * A sampled query vector is a copy of an indexed document's vector, so that document is its own nearest neighbour and every run
         * would trivially agree on it, inflating recall by 1/k. One extra hit is requested so that dropping it still leaves a full top-k.
         */
        final int searchSize;

        private final Map<String, Exception> failures = new HashMap<>();
        private final Map<String, BaselineResult> baselines = new HashMap<>();
        private final List<List<Double>> candidateRecalls;
        private final List<List<Double>> candidateValueRecalls;
        @Nullable
        private final KnnEvalFidelity fidelity;
        private final FieldContext fieldContext;
        private final List<List<Double>> candidateMaxEpsilons;
        private final long[] candidateInfiniteCounts;
        private final List<double[]> candidateRankEpsilonSums;
        private final List<int[]> candidateRankEpsilonCounts;
        private final List<Map<String, KnnEvalResponse.QueryDetail>> details;
        private final List<Long> baselineTookMillis = new ArrayList<>();
        private final List<List<Long>> candidateTookMillis;
        private final List<Long> baselineVectorOps = new ArrayList<>();
        private final List<List<Long>> candidateVectorOps;

        private List<KnnEvalQuery> evaluableQueries;

        KnnEvalState(KnnEvalSpec spec, boolean excludeQueryDocument, List<KnnEvalQuery> queries, FieldContext fieldContext) {
            this.spec = spec;
            this.fieldContext = fieldContext;
            this.fidelity = fieldContext.fidelity();
            this.excludeQueryDocument = excludeQueryDocument;
            this.queries = queries;
            this.searchSize = excludeQueryDocument ? spec.getK() + 1 : spec.getK();
            int numCandidates = spec.getCandidates().size();
            this.candidateRecalls = new ArrayList<>(numCandidates);
            this.candidateValueRecalls = new ArrayList<>(numCandidates);
            this.candidateMaxEpsilons = new ArrayList<>(numCandidates);
            this.candidateInfiniteCounts = new long[numCandidates];
            this.candidateRankEpsilonSums = new ArrayList<>(numCandidates);
            this.candidateRankEpsilonCounts = new ArrayList<>(numCandidates);
            this.details = new ArrayList<>(numCandidates);
            this.candidateTookMillis = new ArrayList<>(numCandidates);
            this.candidateVectorOps = new ArrayList<>(numCandidates);
            for (int c = 0; c < numCandidates; c++) {
                details.add(new HashMap<>());
                candidateRecalls.add(new ArrayList<>());
                candidateValueRecalls.add(new ArrayList<>());
                candidateMaxEpsilons.add(new ArrayList<>());
                candidateRankEpsilonSums.add(new double[spec.getK()]);
                candidateRankEpsilonCounts.add(new int[spec.getK()]);
                candidateTookMillis.add(new ArrayList<>());
                candidateVectorOps.add(new ArrayList<>());
            }
        }

        /**
         * Folds in one batch of the baseline pass. Everything needed is copied out here: {@code multiSearchResponse} owns its pooled hits
         * and releases them once this call's caller returns.
         */
        void addBaselineBatch(MultiSearchResponse multiSearchResponse, List<KnnEvalQuery> batch) {
            Item[] items = multiSearchResponse.getResponses();
            assert items.length == batch.size() : items.length + " != " + batch.size();
            for (int q = 0; q < batch.size(); q++) {
                KnnEvalQuery query = batch.get(q);
                Item item = items[q];
                if (item.isFailure()) {
                    failures.put(query.getId(), item.getFailure());
                    continue;
                }
                baselineTookMillis.add(item.getResponse().getTook().millis());
                baselineVectorOps.add(baselineVectorOperations(item.getResponse()));
                SearchHit[] baselineHits = topKExcluding(
                    item.getResponse().getHits().getHits(),
                    excludeQueryDocument ? query.getId() : null,
                    spec.getK()
                );
                baselines.put(query.getId(), baselineOf(baselineHits));
            }
        }

        /** The queries that have a reference result, in request order. Fixed once the baseline pass has finished. */
        List<KnnEvalQuery> evaluableQueries() {
            if (evaluableQueries == null) {
                List<KnnEvalQuery> surviving = new ArrayList<>(baselines.size());
                for (KnnEvalQuery query : queries) {
                    if (baselines.containsKey(query.getId())) {
                        surviving.add(query);
                    }
                }
                evaluableQueries = surviving;
            }
            return evaluableQueries;
        }

        /** Folds in one batch of one candidate's pass, scoring each query against the reference result stored for it. */
        void addCandidateBatch(int candidateIndex, MultiSearchResponse multiSearchResponse, List<KnnEvalQuery> batch) {
            Item[] items = multiSearchResponse.getResponses();
            assert items.length == batch.size() : items.length + " != " + batch.size();
            for (int q = 0; q < batch.size(); q++) {
                KnnEvalQuery query = batch.get(q);
                Item item = items[q];
                if (item.isFailure()) {
                    failures.putIfAbsent(query.getId(), item.getFailure());
                    continue;
                }
                candidateTookMillis.get(candidateIndex).add(item.getResponse().getTook().millis());
                candidateVectorOps.get(candidateIndex).add(vectorOperationsCount(item.getResponse()));
                SearchHit[] candidateHits = topKExcluding(
                    item.getResponse().getHits().getHits(),
                    excludeQueryDocument ? query.getId() : null,
                    spec.getK()
                );
                KnnEvalResponse.QueryDetail detail = recallOf(
                    query.getId(),
                    candidateHits,
                    baselines.get(query.getId()),
                    fidelity,
                    spec.getK(),
                    spec.getValueTolerance()
                );
                candidateRecalls.get(candidateIndex).add(detail.recall());
                if (detail.recallValue() != null) {
                    candidateValueRecalls.get(candidateIndex).add(detail.recallValue());
                }
                addFidelity(candidateIndex, detail);
                if (spec.isIncludeDetails()) {
                    details.get(candidateIndex).put(query.getId(), detail);
                }
            }
        }

        /**
         * Folds one query's fidelity profile in. A query with an unbounded loss anywhere is counted separately rather than clamped: a
         * clamped value would silently drag the percentiles towards whatever bound was chosen.
         */
        private void addFidelity(int candidateIndex, KnnEvalResponse.QueryDetail detail) {
            if (detail.epsilonProfile().isEmpty()) {
                return;
            }
            if (detail.incomplete()) {
                candidateInfiniteCounts[candidateIndex]++;
            }
            double maxEpsilon = 0.0;
            double[] sums = candidateRankEpsilonSums.get(candidateIndex);
            int[] counts = candidateRankEpsilonCounts.get(candidateIndex);
            for (int rank = 0; rank < detail.epsilonProfile().size(); rank++) {
                Double epsilon = detail.epsilonProfile().get(rank);
                if (epsilon == null) {
                    continue;
                }
                sums[rank] += epsilon;
                counts[rank]++;
                maxEpsilon = Math.max(maxEpsilon, epsilon);
            }
            if (detail.incomplete() == false) {
                candidateMaxEpsilons.get(candidateIndex).add(maxEpsilon);
            }
        }

        @Nullable
        private KnnEvalResponse.Fidelity fidelityOf(int candidateIndex) {
            if (fidelity == null) {
                return null;
            }
            if (fidelity.isSkipped()) {
                return KnnEvalResponse.Fidelity.skipped(fidelity.skippedReason());
            }
            double[] sums = candidateRankEpsilonSums.get(candidateIndex);
            int[] counts = candidateRankEpsilonCounts.get(candidateIndex);
            List<Double> meanByRank = new ArrayList<>(sums.length);
            for (int rank = 0; rank < sums.length; rank++) {
                meanByRank.add(counts[rank] == 0 ? null : sums[rank] / counts[rank]);
            }
            return KnnEvalResponse.Fidelity.of(
                KnnEvalResponse.DoubleStats.of(candidateMaxEpsilons.get(candidateIndex)),
                candidateInfiniteCounts[candidateIndex],
                meanByRank
            );
        }

        /**
         * Vector comparisons for one baseline search. An exact run counts the documents it scanned; an approximate one is profiled.
         */
        private long baselineVectorOperations(SearchResponse searchResponse) {
            if (spec.getBaseline().isExact() == false) {
                return vectorOperationsCount(searchResponse);
            }
            TotalHits totalHits = searchResponse.getHits().getTotalHits();
            // the query document is scanned like any other: excluding it from the hit list happens afterwards
            return totalHits == null ? 0L : totalHits.value();
        }

        /**
         * Total vector comparisons for one search, summed over shards. Only the dfs-phase knn profile carries this, and only for index
         * types that count comparisons, so a missing count contributes nothing rather than failing the request.
         */
        private static long vectorOperationsCount(SearchResponse searchResponse) {
            SearchProfileResults profileResults = searchResponse.getSearchProfileResults();
            if (profileResults == null) {
                return 0L;
            }
            long total = 0L;
            for (SearchProfileShardResult shardResult : profileResults.getShardResults().values()) {
                SearchProfileDfsPhaseResult dfsPhaseResult = shardResult.getSearchProfileDfsPhaseResult();
                if (dfsPhaseResult == null || dfsPhaseResult.getQueryProfileShardResult() == null) {
                    continue;
                }
                for (QueryProfileShardResult queryProfileShardResult : dfsPhaseResult.getQueryProfileShardResult()) {
                    Long vectorOperations = queryProfileShardResult.getVectorOperationsCount();
                    if (vectorOperations != null) {
                        total += vectorOperations;
                    }
                }
            }
            return total;
        }

        /** Echoes a knob set with what the search will actually do, when the mapping was readable and the run is not exact. */
        private KnnEvalResponse.EffectiveKnobs effectiveKnobs(KnnEvalKnobs knobs) {
            if (fieldContext.rescore() == null || knobs.isExact()) {
                return KnnEvalResponse.EffectiveKnobs.of(knobs);
            }
            return new KnnEvalResponse.EffectiveKnobs(
                knobs,
                fieldContext.rescore().effectiveNumCandidates(searchSize, knobs.getNumCandidates(), knobs.getOversample()),
                fieldContext.rescore().rescoreWindow(searchSize, knobs.getOversample())
            );
        }

        KnnEvalResponse buildResponse() {
            List<KnnEvalKnobs> candidates = spec.getCandidates();
            List<KnnEvalResponse.CandidateResult> results = new ArrayList<>(candidates.size());
            for (int c = 0; c < candidates.size(); c++) {
                // A candidate whose every query failed reports 0.0; the failures map is what tells the caller to distrust it. The
                // headline recall is taken from the distribution so that the two can never disagree.
                KnnEvalResponse.DoubleStats recallStats = KnnEvalResponse.DoubleStats.of(candidateRecalls.get(c));
                KnnEvalResponse.DoubleStats valueStats = fidelity == null || fidelity.isSkipped()
                    ? null
                    : KnnEvalResponse.DoubleStats.of(candidateValueRecalls.get(c));
                results.add(
                    new KnnEvalResponse.CandidateResult(
                        effectiveKnobs(candidates.get(c)),
                        recallStats.mean(),
                        recallStats,
                        KnnEvalResponse.RecallBucket.histogram(candidateRecalls.get(c), spec.getK()),
                        KnnEvalResponse.RecallBucket.isBinned(spec.getK()) ? KnnEvalResponse.RecallBucket.BIN_WIDTH : null,
                        valueStats == null ? null : valueStats.mean(),
                        valueStats,
                        fidelity != null && fidelity.isSkipped() ? fidelity.skippedReason() : null,
                        fidelityOf(c),
                        KnnEvalResponse.LongStats.of(candidateTookMillis.get(c)),
                        KnnEvalResponse.LongStats.of(candidateVectorOps.get(c)),
                        details.get(c)
                    )
                );
            }
            Map<String, KnnEvalResponse.BaselineDetail> baselineDetails = Map.of();
            if (spec.isIncludeDetails()) {
                baselineDetails = Maps.newMapWithExpectedSize(baselines.size());
                for (Map.Entry<String, BaselineResult> baseline : baselines.entrySet()) {
                    baselineDetails.put(baseline.getKey(), new KnnEvalResponse.BaselineDetail(baseline.getValue().hits()));
                }
            }
            return new KnnEvalResponse(
                effectiveKnobs(spec.getBaseline()),
                KnnEvalResponse.LongStats.of(baselineTookMillis),
                KnnEvalResponse.LongStats.of(baselineVectorOps),
                spec.getBaseline().isExact() ? KnnEvalResponse.FULL_PRECISION_SCAN : KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
                baselineDetails,
                fidelity == null ? null : spec.getValueTolerance(),
                results,
                failures
            );
        }
    }
}
