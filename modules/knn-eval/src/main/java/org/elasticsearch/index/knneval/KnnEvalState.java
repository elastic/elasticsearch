/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Everything one request accumulates, keyed by query id so that a query dropped by a baseline failure does not shift the rest.
 * Per-query values are kept across batches and summarised once in {@link #buildResponse()}, since batches differ in size and in how
 * many of their queries succeeded.
 */
class KnnEvalState {

    final KnnEvalSpec spec;
    final boolean excludeQueryDocument;
    final List<KnnEvalQuery> queries;
    /** A sampled query's own document is dropped from both lists, so one extra hit is requested to still leave a full top-k. */
    final int searchSize;

    private final Map<String, Exception> failures = new HashMap<>();
    private final Map<String, KnnEvalRecall.BaselineResult> baselines = new HashMap<>();
    private final List<List<Double>> candidateRecalls;
    private final List<List<Double>> candidateValueRecalls;
    @Nullable
    private final KnnEvalFidelity fidelity;
    private final KnnEvalFieldContext fieldContext;
    private final List<List<Double>> candidateMaxEpsilons;
    private final long[] candidateInfiniteCounts;
    private final List<double[]> candidateRankEpsilonSums;
    private final List<int[]> candidateRankEpsilonCounts;
    private final List<Map<String, KnnEvalDetails.QueryDetail>> details;
    private final List<Long> baselineTookMillis = new ArrayList<>();
    private final List<List<Long>> candidateTookMillis;
    private final List<Long> baselineVectorOps = new ArrayList<>();
    private final List<List<Long>> candidateVectorOps;

    private List<KnnEvalQuery> evaluableQueries;

    KnnEvalState(KnnEvalSpec spec, boolean excludeQueryDocument, List<KnnEvalQuery> queries, KnnEvalFieldContext fieldContext) {
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
            SearchHit[] baselineHits = KnnEvalRecall.topKExcluding(
                item.getResponse().getHits().getHits(),
                excludeQueryDocument ? query.getId() : null,
                spec.getK()
            );
            baselines.put(query.getId(), KnnEvalRecall.baselineOf(baselineHits));
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
            SearchHit[] candidateHits = KnnEvalRecall.topKExcluding(
                item.getResponse().getHits().getHits(),
                excludeQueryDocument ? query.getId() : null,
                spec.getK()
            );
            KnnEvalDetails.QueryDetail detail = KnnEvalRecall.recallOf(
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
    private void addFidelity(int candidateIndex, KnnEvalDetails.QueryDetail detail) {
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
    private KnnEvalFidelity.Summary fidelityOf(int candidateIndex) {
        if (fidelity == null) {
            return null;
        }
        if (fidelity.isSkipped()) {
            return KnnEvalFidelity.Summary.skipped(fidelity.skippedReason());
        }
        double[] sums = candidateRankEpsilonSums.get(candidateIndex);
        int[] counts = candidateRankEpsilonCounts.get(candidateIndex);
        List<Double> meanByRank = new ArrayList<>(sums.length);
        for (int rank = 0; rank < sums.length; rank++) {
            meanByRank.add(counts[rank] == 0 ? null : sums[rank] / counts[rank]);
        }
        return KnnEvalFidelity.Summary.of(
            KnnEvalStats.of(candidateMaxEpsilons.get(candidateIndex)),
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
            KnnEvalStats recallStats = KnnEvalStats.of(candidateRecalls.get(c));
            KnnEvalStats valueStats = fidelity == null || fidelity.isSkipped() ? null : KnnEvalStats.of(candidateValueRecalls.get(c));
            results.add(
                new KnnEvalResponse.KnnSettingsResult(
                    effectiveKnobs(candidates.get(c)),
                    recallStats.mean(),
                    recallStats,
                    spec.isIncludeHistogram() ? RecallBucket.histogram(candidateRecalls.get(c), spec.getK()) : null,
                    spec.isIncludeHistogram() && RecallBucket.isBinned(spec.getK()) ? RecallBucket.BIN_WIDTH : null,
                    valueStats == null ? null : valueStats.mean(),
                    valueStats,
                    fidelity != null && fidelity.isSkipped() ? fidelity.skippedReason() : null,
                    fidelityOf(c),
                    KnnEvalStats.of(candidateTookMillis.get(c)),
                    KnnEvalStats.of(candidateVectorOps.get(c)),
                    details.get(c)
                )
            );
        }
        Map<String, KnnEvalDetails.BaselineDetail> baselineDetails = Map.of();
        if (spec.isIncludeDetails()) {
            baselineDetails = Maps.newMapWithExpectedSize(baselines.size());
            for (Map.Entry<String, KnnEvalRecall.BaselineResult> baseline : baselines.entrySet()) {
                baselineDetails.put(baseline.getKey(), new KnnEvalDetails.BaselineDetail(baseline.getValue().hits()));
            }
        }
        return new KnnEvalResponse(
            effectiveKnobs(spec.getBaseline()),
            KnnEvalStats.of(baselineTookMillis),
            KnnEvalStats.of(baselineVectorOps),
            spec.getBaseline().isExact() ? KnnEvalResponse.FULL_PRECISION_SCAN : KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            baselineDetails,
            fieldContext.environment(),
            fidelity == null ? null : spec.getValueTolerance(),
            results,
            failures
        );
    }
}
