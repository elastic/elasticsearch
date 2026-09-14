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
 * Estimates ANN recall without brute-force ground truth: each query runs under the baseline knobs and then under each candidate, and
 * {@link RecallAtK} scores the overlap using the baseline's top-k as derived {@link RatedDocument} judgements.
 * <p>
 * All passes share one point-in-time, so a concurrent refresh cannot masquerade as a recall difference, and each pass is homogeneous so
 * that an expensive baseline search cannot steal search threads from whichever candidate was scheduled beside it. {@code took_ms}
 * therefore reflects an index the baseline pass has already warmed; {@code vector_ops} is the cache-independent axis.
 */
public class TransportKnnEvalAction extends HandledTransportAction<KnnEvalRequest, KnnEvalResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKnnEvalAction.class);

    /** {@link RecallAtK}'s default threshold is also 1, so every baseline hit counts as relevant. */
    private static final int RELEVANT_RATING = 1;

    /** Held for the whole sweep -- every batch of every pass -- and never refreshed. */
    static final TimeValue POINT_IN_TIME_KEEP_ALIVE = TimeValue.timeValueMinutes(5);

    /** Any positive value makes an exact query score on the real vectors rather than the quantized ones. */
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
            // a full scan is what that setting exists to keep off a cluster; an approximate baseline is an ordinary kNN search
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

    /** Either half is {@code null} when the mapping could not be read. */
    record FieldContext(@Nullable KnnEvalFidelity fidelity, @Nullable KnnEvalRescore rescore) {
        static final FieldContext EMPTY = new FieldContext(null, null);
    }

    /**
     * Reads the field's mapping, which the similarity-based metrics, the resolved candidate windows and the knob compatibility checks
     * all need. It requires {@code view_index_metadata}, so a failed lookup drops those rather than failing the request.
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
                // ActionListener.run is what turns a validation failure below into a response
                ActionListener.run(listener, l -> l.onResponse(fieldContextOf(response)));
            }

            private FieldContext fieldContextOf(GetFieldMappingsResponse response) {
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
                KnnEvalRescore rescore = fieldMapping == null ? null : KnnEvalRescore.fromFieldMapping(fieldMapping);
                if (rescore != null) {
                    // a knob the field would ignore yields a sweep in which nothing varied, which reads as good news
                    rescore.validateSupportedKnobs(spec.getBaseline());
                    for (KnnEvalKnobs knobs : spec.getKnnSettings()) {
                        rescore.validateSupportedKnobs(knobs);
                    }
                }
                // a mapping that is present but wrong is a caller error, unlike one we could not read
                KnnEvalFidelity fidelity = spec.isIncludeFidelity()
                    ? KnnEvalFidelity.fromFieldMapping(field, fieldMapping, rescore, baselineOversample(spec.getBaseline()))
                    : null;
                return new FieldContext(fidelity, rescore);
            }

            @Override
            public void onFailure(Exception e) {
                if (spec.isIncludeFidelity()) {
                    // asked for by name, and not computable without the mapping
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

    /** An exact baseline scores on the real vectors, so it lifts the rescoring guard just as an explicit knob does. */
    @Nullable
    private static Float baselineOversample(KnnEvalKnobs baseline) {
        // boxed: a float branch would unbox the null one
        return baseline.isExact() ? Float.valueOf(EXACT_SCORING_OVERSAMPLE) : baseline.getOversample();
    }

    private void openPointInTime(Task task, KnnEvalRequest request, FieldContext fieldContext, ActionListener<KnnEvalResponse> listener) {
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(request.indices()).indicesOptions(request.indicesOptions())
            .keepAlive(POINT_IN_TIME_KEEP_ALIVE);
        client.execute(TransportOpenPointInTimeAction.TYPE, openRequest, listener.delegateFailureAndWrap((delegate, openResponse) -> {
            BytesReference pointInTimeId = openResponse.getPointInTimeId();
            // runAfter fires either way and ActionListener.run funnels throws into onFailure: no path leaves the PIT open
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
                // the keep-alive expires anyway, so this costs search context memory and nothing else
                logger.warn("failed to close the point in time opened for kNN evaluation", e);
            }
        });
    }

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
                // a recall of zero would read as a catastrophic candidate rather than an empty index or a wrong field name
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
     * Phase 1: baseline searches only, one batch at a time. Batching, not concurrency, is what bounds heap -- a coordinator holds every
     * sub-search response of one msearch until the last arrives. Recursing inside the callback is safe because the state copies out
     * what it needs synchronously, so each batch's response is released before the next callback runs.
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

    /** Phase 2: one homogeneous pass per knn_settings entry, in the order the caller listed them. */
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
        if (candidateIndex >= state.spec.getKnnSettings().size()) {
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
        KnnEvalKnobs candidate = state.spec.getKnnSettings().get(candidateIndex);
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
        // defaults to 1, so each reported took is one search's shard time rather than contention with its siblings
        msearchRequest.maxConcurrentSearchRequests(spec.getMaxConcurrentSearches());
        return msearchRequest;
    }

    /**
     * Samples query vectors from the corpus, which keeps the query distribution matched to the indexed one. It runs through the same
     * point-in-time, so a sampled document is searchable in every pass. {@link KnnEvalSpec#getFilter()} is deliberately not applied:
     * drawing queries from the filtered subset would make a restrictive filter look harmless.
     */
    private static SearchRequest buildSampleRequest(KnnEvalSpec spec, KnnEvalSample sample, BytesReference pointInTimeId) {
        RandomScoreFunctionBuilder randomScore = new RandomScoreFunctionBuilder();
        if (sample.getSeed() != null) {
            // `field` is compulsory once a seed is set, and `_seq_no` is unique per document within a shard
            randomScore.seed(sample.getSeed()).setField(SeqNoFieldMapper.NAME);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), randomScore)
        ).size(sample.getSize()).fetchSource(false).fetchField(spec.getField()).pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return new SearchRequest().source(source);
    }

    /** Read eagerly: the response's pooled hits are released as soon as this callback returns. */
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

    /** @return {@code null} for a document with no vector, which is simply not usable as a query */
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

    /** No indices or indices options: {@link SearchRequest#validate()} rejects either alongside a point-in-time. */
    private static SearchRequest buildSearch(
        KnnEvalSpec spec,
        KnnEvalQuery query,
        KnnEvalKnobs knobs,
        int searchSize,
        BytesReference pointInTimeId
    ) {
        if (knobs.isExact()) {
            return new SearchRequest().source(
                // exact_knn is not profiled, so vector ops = matched docs: one full-precision comparison each
                new SearchSourceBuilder().query(exactQuery(spec, query))
                    .size(searchSize)
                    .fetchSource(false)
                    .trackTotalHitsUpTo(Integer.MAX_VALUE)
                    .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId))
            );
        }
        // num_candidates is validated against k, but a sampled query's extra hit pushes the window one past it
        Integer numCandidates = knobs.getNumCandidates() == null ? null : Math.max(knobs.getNumCandidates(), searchSize);
        KnnSearchBuilder.Builder knnSearch = new KnnSearchBuilder.Builder().field(spec.getField())
            .queryVector(query.getQueryVector())
            .k(searchSize)
            .numCandidates(numCandidates)
            .visitPercentage(knobs.getVisitPercentage())
            // null leaves the field mapping's own rescoring in force
            .rescoreVectorBuilder(knobs.getOversample() == null ? null : new RescoreVectorBuilder(knobs.getOversample()));
        if (spec.getFilter() != null) {
            knnSearch.addFilterQueries(List.of(spec.getFilter()));
        }
        // The knn section rather than the equivalent knn query: only the dfs-phase path records vector_operations_count, which is why
        // profile is on. Builder.build(size) applies the same 1.5 * k num_candidates default the query form would.
        SearchSourceBuilder source = new SearchSourceBuilder().knnSearch(List.of(knnSearch.build(searchSize)))
            .size(searchSize)
            .fetchSource(false)
            .profile(true)
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return new SearchRequest().source(source);
    }

    /**
     * Brute force over every document with a vector. The oversample argument oversamples nothing here -- it only selects scoring
     * fidelity, so passing it explicitly keeps an exact baseline full precision even where the mapping has rescoring off.
     * {@link ExactKnnQueryBuilder} carries no filter of its own, hence the bool wrapper.
     */
    private static QueryBuilder exactQuery(KnnEvalSpec spec, KnnEvalQuery query) {
        QueryBuilder exactKnn = new ExactKnnQueryBuilder(query.getQueryVector(), spec.getField(), null, EXACT_SCORING_OVERSAMPLE);
        if (spec.getFilter() == null) {
            return exactKnn;
        }
        return new BoolQueryBuilder().must(exactKnn).filter(spec.getFilter());
    }

    /**
     * A query's reference result, carried from the baseline pass into every candidate pass. {@code ratedDocs} is built while the live
     * hits still expose their index name, since the overlap is keyed on {@code _index}/{@code _id} and not on {@code _id} alone.
     */
    record BaselineResult(List<KnnEvalResponse.Hit> hits, List<RatedDocument> ratedDocs) {}

    /** The live hits do not outlive the msearch callback, so the reference is captured here. */
    static BaselineResult baselineOf(SearchHit[] baselineHits) {
        List<RatedDocument> ratedDocs = new ArrayList<>(baselineHits.length);
        for (SearchHit hit : baselineHits) {
            ratedDocs.add(new RatedDocument(hit.getIndex(), hit.getId(), RELEVANT_RATING));
        }
        return new BaselineResult(toHits(baselineHits), ratedDocs);
    }

    /**
     * Scores a candidate run against the stored baseline and annotates each hit with its baseline rank, which is what turns a bare
     * recall number into something actionable. {@code relevant} is the baseline hit count rather than {@code k}, so a shard that cannot
     * return {@code k} documents yields 1.0 rather than a misleading fraction.
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

            // both lists are score-ordered, so rank i is comparable with rank i without matching ids
            List<Double> epsilonProfile = List.of();
            boolean incomplete = false;
            if (fidelity != null && fidelity.isSkipped() == false) {
                List<Double> profile = new ArrayList<>(k);
                for (int rank = 0; rank < k; rank++) {
                    if (rank >= baseline.hits().size()) {
                        // the reference never reached this rank, so there is nothing to have lost
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
            // shares the id recall's denominator, so the gap between the two is meaningful
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
            // RecallAtK inc-refs every hit via RatedSearchHit; the response owns the hits, so drop the extra refs here
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

    /** Both runs are trimmed identically, so the overlap is over comparable windows. */
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
     * Everything one request accumulates, keyed by query id so that a query dropped by a baseline failure does not shift the rest.
     * Per-query values are kept across batches and summarised once in {@link #buildResponse()}, since batches differ in size and in how
     * many of their queries succeeded.
     */
    static class KnnEvalState {

        final KnnEvalSpec spec;
        final boolean excludeQueryDocument;
        final List<KnnEvalQuery> queries;
        /** A sampled query's own document is dropped from both lists, so one extra hit is requested to still leave a full top-k. */
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
            int numCandidates = spec.getKnnSettings().size();
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

        /** Copies out everything needed: the response owns its pooled hits and releases them once the caller returns. */
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

        /** The queries with a reference result, in request order; fixed once the baseline pass has finished. */
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

        /** A query with an unbounded loss is counted separately: clamping would drag the percentiles towards the chosen bound. */
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

        /** An exact run counts the documents it scanned; an approximate one is profiled. */
        private long baselineVectorOperations(SearchResponse searchResponse) {
            if (spec.getBaseline().isExact() == false) {
                return vectorOperationsCount(searchResponse);
            }
            TotalHits totalHits = searchResponse.getHits().getTotalHits();
            // the query document is scanned like any other; the exclusion is post-hoc
            return totalHits == null ? 0L : totalHits.value();
        }

        /** Only the dfs-phase knn profile carries this, so a missing count contributes nothing rather than failing. */
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

        /** Echoes what the search will actually do, when the mapping was readable and the run is not exact. */
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
            List<KnnEvalKnobs> candidates = spec.getKnnSettings();
            List<KnnEvalResponse.KnnSettingsResult> results = new ArrayList<>(candidates.size());
            for (int c = 0; c < candidates.size(); c++) {
                // a candidate whose every query failed reports 0.0; the failures map is what says to distrust it
                KnnEvalResponse.DoubleStats recallStats = KnnEvalResponse.DoubleStats.of(candidateRecalls.get(c));
                KnnEvalResponse.DoubleStats valueStats = fidelity == null || fidelity.isSkipped()
                    ? null
                    : KnnEvalResponse.DoubleStats.of(candidateValueRecalls.get(c));
                results.add(
                    new KnnEvalResponse.KnnSettingsResult(
                        effectiveKnobs(candidates.get(c)),
                        recallStats.mean(),
                        recallStats,
                        spec.isIncludeHistogram() ? KnnEvalResponse.RecallBucket.histogram(candidateRecalls.get(c), spec.getK()) : null,
                        spec.isIncludeHistogram() && KnnEvalResponse.RecallBucket.isBinned(spec.getK())
                            ? KnnEvalResponse.RecallBucket.BIN_WIDTH
                            : null,
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
