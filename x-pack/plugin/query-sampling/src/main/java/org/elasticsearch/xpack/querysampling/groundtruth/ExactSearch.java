/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;

import java.util.Arrays;
import java.util.List;

/**
 * Turns a captured kNN search into the search that finds its true answer. It scans the index, so it must stay off the
 * search path.
 */
public final class ExactSearch {

    // any positive oversample makes quantized fields be scored on full precision vectors, otherwise the "exact" answer
    // would itself be approximate; fields that are not quantized ignore it
    private static final float FULL_PRECISION = 1f;

    private ExactSearch() {}

    public static SearchRequest request(CapturedQuery query) {
        VectorData vector = new VectorData(query.queryVector(), null, null);
        BoolQueryBuilder exact = QueryBuilders.boolQuery().must(new ExactKnnQueryBuilder(vector, query.field(), null, FULL_PRECISION));
        for (QueryBuilder filter : query.filters()) {
            exact.filter(filter);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(exact).size(query.k()).fetchSource(false);
        // an answer without the shards that failed would miss neighbours that they hold, and be kept as the true one
        return new SearchRequest(query.indices()).source(source).allowPartialSearchResults(false);
    }

    /**
     * Copies the answer out of the ref-counted response. The data state is read separately.
     */
    public static GroundTruth parse(SearchResponse response) {
        SearchHit[] searchHits = response.getHits().getHits();
        List<CapturedSearch.Hit> neighbors = Arrays.stream(searchHits)
            .map(hit -> new CapturedSearch.Hit(hit.getIndex(), hit.getId(), hit.getScore()))
            .toList();
        return new GroundTruth(neighbors, null);
    }
}
