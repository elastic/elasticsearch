/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.apache.lucene.search.TotalHits;
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
    /** Whether each query is a stored document's vector, so that document must be dropped from its own results. */
    final boolean queryIsSampledFromDocuments;
    final List<KnnEvalQuery> queries;
    final int searchSize;

    private final KnnEvalRescore rescore;
    private final Map<String, KnnEvalRecall.BaselineResult> baselines = new HashMap<>();
    private final Map<String, Exception> failures = new HashMap<>();
    private final List<SettingAccumulator> settings;

    private long baselineTookMillis;
    private long baselineVectorOps;
    private List<KnnEvalQuery> evaluableQueries;

    KnnEvalState(KnnEvalSpec spec, boolean queryIsSampledFromDocuments, List<KnnEvalQuery> queries, KnnEvalRescore rescore) {
        this.spec = spec;
        this.queryIsSampledFromDocuments = queryIsSampledFromDocuments;
        this.queries = List.copyOf(queries);
        this.rescore = rescore;
        // the sampled document comes back and is dropped, so fetch one extra to leave k
        this.searchSize = queryIsSampledFromDocuments ? spec.getK() + 1 : spec.getK();
        this.settings = new ArrayList<>(spec.getKnnSettings().size());
        for (int setting = 0; setting < spec.getKnnSettings().size(); setting++) {
            settings.add(new SettingAccumulator());
        }
    }

    /** Records one query's failure; the sweep continues. */
    void addFailure(KnnEvalQuery query, Exception failure) {
        failures.putIfAbsent(query.getId(), failure);
    }

    void addBaseline(KnnEvalQuery query, SearchResponse response) {
        baselineTookMillis += response.getTook().millis();
        baselineVectorOps += baselineVectorOperations(response);
        SearchHit[] baselineHits = KnnEvalRecall.topKExcluding(
            response.getHits().getHits(),
            queryIsSampledFromDocuments ? query.getId() : null,
            spec.getK()
        );
        if (baselineHits.length < spec.getK()) {
            addFailure(
                query,
                new IllegalArgumentException(
                    "baseline returned ["
                        + baselineHits.length
                        + "] hits, fewer than [k="
                        + spec.getK()
                        + "]; recall@k is undefined for this query"
                )
            );
            return;
        }
        baselines.put(query.getId(), KnnEvalRecall.baselineOf(baselineHits));
    }

    /** Queries with a baseline result, in request order. */
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

    void addCandidate(int settingIndex, KnnEvalQuery query, SearchResponse response) {
        long tookMs = response.getTook().millis();
        long operations = vectorOperationsCount(response);
        SearchHit[] candidateHits = KnnEvalRecall.topKExcluding(
            response.getHits().getHits(),
            queryIsSampledFromDocuments ? query.getId() : null,
            spec.getK()
        );
        boolean scoresComparable = rescore.returnsQuantizedScores(spec.getBaseline()) == false
            && rescore.returnsQuantizedScores(spec.getKnnSettings().get(settingIndex)) == false;
        KnnEvalRecall.RecallResult result = KnnEvalRecall.recallOf(
            candidateHits,
            baselines.get(query.getId()),
            scoresComparable,
            tookMs,
            operations
        );
        settings.get(settingIndex).add(result);
    }

    KnnEvalResponse buildResponse() {
        List<KnnEvalResponse.KnnSettingsResult> results = new ArrayList<>(spec.getKnnSettings().size());
        for (int setting = 0; setting < spec.getKnnSettings().size(); setting++) {
            results.add(settings.get(setting).result(reportedSettings(spec.getKnnSettings().get(setting))));
        }
        return new KnnEvalResponse(
            spec.getQuerySource().from(),
            reportedSettings(spec.getBaseline()),
            baselineTookMillis,
            baselineVectorOps,
            spec.getBaseline().isExact() ? KnnEvalResponse.FULL_PRECISION_SCAN : KnnEvalResponse.QUANTIZED_VISIT_PLUS_RESCORE,
            results,
            failures
        );
    }

    private KnnEvalResponse.ReportedSettings reportedSettings(KnnEvalSettings knnSettings) {
        boolean capped = knnSettings.isExact() == false
            && (rescore.autoCalibrate() == false || knnSettings.getRescoreOversample() != null)
            && rescore.isRescoreWindowCapped(searchSize, knnSettings.getRescoreOversample());
        return new KnnEvalResponse.ReportedSettings(knnSettings, capped, rescore.returnsQuantizedScores(knnSettings));
    }

    /** Exact counts scanned docs; approximate is profiled. */
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

        private KnnEvalResponse.KnnSettingsResult result(KnnEvalResponse.ReportedSettings knnSettings) {
            return new KnnEvalResponse.KnnSettingsResult(
                knnSettings,
                includedQueries == 0 ? null : recallSum / includedQueries,
                includedQueries,
                excludedQueries,
                tookMillis,
                vectorOps
            );
        }
    }
}
