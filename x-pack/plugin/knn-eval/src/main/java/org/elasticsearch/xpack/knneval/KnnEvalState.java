/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Retains bounded aggregates after each pooled search response is released. */
final class KnnEvalState {

    final KnnEvalSpec spec;
    final boolean excludeQueryDocument;
    final List<KnnEvalQuery> queries;
    final int searchSize;

    private final KnnEvalRescore rescore;
    private final Map<String, KnnEvalRecall.BaselineResult> baselines = new HashMap<>();
    private final Map<String, Exception> failures = new HashMap<>();
    private final List<SettingAccumulator> settings;

    private long baselineTookMillis;
    private long baselineVectorOps;
    private List<KnnEvalQuery> evaluableQueries;

    KnnEvalState(KnnEvalSpec spec, boolean excludeQueryDocument, List<KnnEvalQuery> queries, KnnEvalRescore rescore) {
        this.spec = spec;
        this.excludeQueryDocument = excludeQueryDocument;
        this.queries = List.copyOf(queries);
        this.rescore = rescore;
        this.searchSize = excludeQueryDocument ? spec.getK() + 1 : spec.getK();
        this.settings = new ArrayList<>(spec.getKnnSettings().size());
        for (int setting = 0; setting < spec.getKnnSettings().size(); setting++) {
            settings.add(new SettingAccumulator());
        }
    }

    void addBaselineBatch(MultiSearchResponse multiSearchResponse, List<KnnEvalQuery> batch) {
        Item[] items = multiSearchResponse.getResponses();
        assert items.length == batch.size() : items.length + " != " + batch.size();
        for (int queryIndex = 0; queryIndex < batch.size(); queryIndex++) {
            KnnEvalQuery query = batch.get(queryIndex);
            Item item = items[queryIndex];
            if (item.isFailure()) {
                failures.putIfAbsent(query.getId(), item.getFailure());
                continue;
            }
            baselineTookMillis += item.getResponse().getTook().millis();
            baselineVectorOps += baselineVectorOperations(item.getResponse());
            SearchHit[] baselineHits = KnnEvalRecall.topKExcluding(
                item.getResponse().getHits().getHits(),
                excludeQueryDocument ? query.getId() : null,
                spec.getK()
            );
            if (baselineHits.length < spec.getK()) {
                failures.putIfAbsent(
                    query.getId(),
                    new IllegalArgumentException(
                        "baseline returned ["
                            + baselineHits.length
                            + "] hits, fewer than [k="
                            + spec.getK()
                            + "]; recall@k is undefined for this query"
                    )
                );
                continue;
            }
            baselines.put(query.getId(), KnnEvalRecall.baselineOf(baselineHits));
        }
    }

    /** The queries with a reference result, in request order. */
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

    void addCandidateBatch(int settingIndex, MultiSearchResponse multiSearchResponse, List<KnnEvalQuery> batch) {
        Item[] items = multiSearchResponse.getResponses();
        assert items.length == batch.size() : items.length + " != " + batch.size();
        for (int queryIndex = 0; queryIndex < batch.size(); queryIndex++) {
            KnnEvalQuery query = batch.get(queryIndex);
            Item item = items[queryIndex];
            if (item.isFailure()) {
                failures.putIfAbsent(query.getId(), item.getFailure());
                continue;
            }
            long tookMs = item.getResponse().getTook().millis();
            long operations = vectorOperationsCount(item.getResponse());
            SearchHit[] candidateHits = KnnEvalRecall.topKExcluding(
                item.getResponse().getHits().getHits(),
                excludeQueryDocument ? query.getId() : null,
                spec.getK()
            );
            KnnEvalRecall.RecallResult result = KnnEvalRecall.recallOf(candidateHits, baselines.get(query.getId()), tookMs, operations);
            settings.get(settingIndex).add(result);
        }
    }

    KnnEvalResponse buildResponse() {
        List<KnnEvalResponse.KnnSettingsResult> results = new ArrayList<>(spec.getKnnSettings().size());
        for (int setting = 0; setting < spec.getKnnSettings().size(); setting++) {
            results.add(settings.get(setting).result(reportedKnobs(spec.getKnnSettings().get(setting))));
        }
        return new KnnEvalResponse(
            reportedKnobs(spec.getBaseline()),
            baselineTookMillis,
            baselineVectorOps,
            spec.getBaseline().isExact() ? KnnEvalResponse.FULL_PRECISION_SCAN : KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            spec.getMaxQueriesPerBatch(),
            results,
            failures
        );
    }

    private KnnEvalResponse.ReportedKnobs reportedKnobs(KnnEvalKnobs knobs) {
        boolean capped = knobs.isExact() == false
            && (rescore.autoCalibrate() == false || knobs.getRescoreOversample() != null)
            && rescore.isRescoreWindowCapped(searchSize, knobs.getRescoreOversample());
        return new KnnEvalResponse.ReportedKnobs(knobs, capped);
    }

    /** An exact run counts the documents it scanned; an approximate one is profiled. */
    private long baselineVectorOperations(SearchResponse searchResponse) {
        if (spec.getBaseline().isExact() == false) {
            return vectorOperationsCount(searchResponse);
        }
        TotalHits totalHits = searchResponse.getHits().getTotalHits();
        return totalHits == null ? 0L : totalHits.value();
    }

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
                Long operations = queryProfileShardResult.getVectorOperationsCount();
                if (operations != null) {
                    total += operations;
                }
            }
        }
        return total;
    }

    private static final class SettingAccumulator {
        private double recallSum;
        private long includedQueries;
        private long excludedQueries;
        private long tookMillis;
        private long vectorOps;

        private void add(KnnEvalRecall.RecallResult result) {
            if (result.recall() == null) {
                excludedQueries++;
            } else {
                recallSum += result.recall();
                includedQueries++;
            }
            tookMillis += result.tookMs();
            vectorOps += result.vectorOps();
        }

        private KnnEvalResponse.KnnSettingsResult result(KnnEvalResponse.ReportedKnobs knobs) {
            return new KnnEvalResponse.KnnSettingsResult(
                knobs,
                includedQueries == 0 ? null : recallSum / includedQueries,
                includedQueries,
                excludedQueries,
                tookMillis,
                vectorOps
            );
        }
    }
}
