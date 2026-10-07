/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.RescoreVectorBuilder;
import org.elasticsearch.search.vectors.VectorData;

import java.util.ArrayList;
import java.util.List;

/** Builds sampling, baseline, and candidate search requests. */
final class KnnEvalSearches {

    private KnnEvalSearches() {}

    /** Any positive value makes an exact query score full-precision vectors. */
    private static final float EXACT_SCORING_OVERSAMPLE = 1.0f;

    /** Samples the vector field so queries match the corpus; the shared PIT keeps them searchable. */
    static SearchRequest buildSampleRequest(KnnEvalSpec spec, KnnEvalSample sample, BytesReference pointInTimeId) {
        RandomScoreFunctionBuilder randomScore = new RandomScoreFunctionBuilder();
        if (sample.getSeed() != null) {
            // a seeded random_score needs a field; _seq_no is unique per shard document
            randomScore.seed(sample.getSeed()).setField(SeqNoFieldMapper.NAME);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.functionScoreQuery(QueryBuilders.existsQuery(spec.getField()), randomScore)
        )
            .size(sample.getSize())
            .fetchSource(false)
            .fetchField(new FieldAndFormat(spec.getField(), "array", null))
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return searchRequest(source);
    }

    static SearchRequest buildVectorCountRequest(KnnEvalSpec spec, BytesReference pointInTimeId) {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.existsQuery(spec.getField()))
            .size(0)
            .trackTotalHits(true)
            .fetchSource(false)
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return searchRequest(source);
    }

    /** Copies vectors out before the pooled response is released. */
    static List<KnnEvalQuery> extractSampledQueries(SearchResponse searchResponse, String field) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<KnnEvalQuery> queries = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            float[] vector = extractVector(hit, field);
            if (vector != null) {
                queries.add(new KnnEvalQuery(KnnEvalRecall.key(hit), VectorData.fromFloats(vector)));
            }
        }
        return queries;
    }

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

    /** PIT searches reject explicit indices, so none are set. */
    static SearchRequest buildSearch(
        KnnEvalSpec spec,
        KnnEvalQuery query,
        KnnEvalSettings knnSettings,
        int searchSize,
        BytesReference pointInTimeId
    ) {
        return knnSettings.isExact()
            ? buildExactSearch(spec, query, searchSize, pointInTimeId)
            : buildApproximateSearch(spec, query, knnSettings, searchSize, pointInTimeId);
    }

    private static SearchRequest buildExactSearch(KnnEvalSpec spec, KnnEvalQuery query, int searchSize, BytesReference pointInTimeId) {
        SearchSourceBuilder source = new SearchSourceBuilder().query(exactQuery(spec, query))
            .size(searchSize)
            .fetchSource(false)
            // exact_knn isn't profiled; matched docs are the vector op count
            .trackTotalHits(true)
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return searchRequest(source);
    }

    private static SearchRequest buildApproximateSearch(
        KnnEvalSpec spec,
        KnnEvalQuery query,
        KnnEvalSettings knnSettings,
        int searchSize,
        BytesReference pointInTimeId
    ) {
        // a sampled query drops its own hit, so search one extra hit and candidate
        int extra = searchSize - spec.getK();
        Integer numCandidates = knnSettings.getNumCandidates() == null ? null : knnSettings.getNumCandidates() + extra;
        KnnSearchBuilder.Builder knnSearch = new KnnSearchBuilder.Builder().field(spec.getField())
            .queryVector(query.getQueryVector())
            .k(searchSize)
            .numCandidates(numCandidates)
            .visitPercentage(knnSettings.getVisitPercentage())
            // null keeps the mapping's rescoring
            .rescoreVectorBuilder(
                knnSettings.getRescoreOversample() == null ? null : new RescoreVectorBuilder(knnSettings.getRescoreOversample())
            );
        // knn section, not the knn query: only the dfs path records vector_operations_count, hence profile(true)
        // build(size) applies the query form's 1.5 * k num_candidates default
        SearchSourceBuilder source = new SearchSourceBuilder().knnSearch(List.of(knnSearch.build(searchSize)))
            .size(searchSize)
            .fetchSource(false)
            .profile(true)
            .pointInTimeBuilder(new PointInTimeBuilder(pointInTimeId));
        return searchRequest(source);
    }

    /** A dropped shard fails the search: partial results would silently shrink the corpus recall describes. */
    private static SearchRequest searchRequest(SearchSourceBuilder source) {
        return new SearchRequest().source(source).allowPartialSearchResults(false);
    }

    /** Brute-force full-precision query, even if the mapping disables rescoring. */
    private static QueryBuilder exactQuery(KnnEvalSpec spec, KnnEvalQuery query) {
        return new ExactKnnQueryBuilder(query.getQueryVector(), spec.getField(), null, EXACT_SCORING_OVERSAMPLE);
    }
}
