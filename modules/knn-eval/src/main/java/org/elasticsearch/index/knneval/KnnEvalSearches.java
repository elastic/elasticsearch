/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;

import java.util.ArrayList;
import java.util.List;

/** Builds the sample, baseline and candidate searches: everything about how a query vector becomes a request. */
final class KnnEvalSearches {

    private KnnEvalSearches() {}

    /** Any positive value makes an exact query score on the real vectors rather than the quantized ones. */
    private static final float EXACT_SCORING_OVERSAMPLE = 1.0f;

    /** An exact baseline scores on the real vectors, so it lifts the rescoring guard just as an explicit knob does. */
    @Nullable
    static Float baselineOversample(KnnEvalKnobs baseline) {
        // boxed: a float branch would unbox the null one
        return baseline.isExact() ? Float.valueOf(EXACT_SCORING_OVERSAMPLE) : baseline.getOversample();
    }

    static MultiSearchRequest newMultiSearchRequest(KnnEvalSpec spec) {
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
    static SearchRequest buildSampleRequest(KnnEvalSpec spec, KnnEvalSample sample, BytesReference pointInTimeId) {
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
    static List<KnnEvalQuery> extractSampledQueries(SearchResponse searchResponse, String field) {
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
    static SearchRequest buildSearch(
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
}
