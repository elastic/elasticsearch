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

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a captured kNN search into the search that finds its true answer, and reads that answer back.
 * <p>
 * The exact search scores every document that has a vector and matches the filters, so it costs a scan of
 * the index and must stay off the search path.
 */
public final class ExactSearch {

    /**
     * An oversample is only passed to make sure quantized fields are scored with their full precision
     * vectors: without one, a quantized field that has no rescoring configured would be scored on its
     * quantized vectors, and the "exact" answer would itself be approximate. The value is irrelevant for
     * the result, any positive number has this effect, and fields that are not quantized ignore it.
     */
    private static final float FULL_PRECISION = 1f;

    private ExactSearch() {}

    public static SearchRequest request(CapturedQuery query) {
        VectorData vector = new VectorData(query.queryVector(), null, null);
        BoolQueryBuilder exact = QueryBuilders.boolQuery().must(new ExactKnnQueryBuilder(vector, query.field(), null, FULL_PRECISION));
        for (QueryBuilder filter : query.filters()) {
            exact.filter(filter);
        }
        SearchSourceBuilder source = new SearchSourceBuilder().query(exact).size(query.k()).fetchSource(false).trackTotalHits(false);
        return new SearchRequest(query.indices()).source(source);
    }

    /**
     * Copies the answer out of the response, which is ref-counted and cannot be kept.
     */
    public static GroundTruth parse(SearchResponse response) {
        SearchHit[] searchHits = response.getHits().getHits();
        List<CapturedSearch.Hit> neighbors = new ArrayList<>(searchHits.length);
        for (SearchHit hit : searchHits) {
            neighbors.add(new CapturedSearch.Hit(hit.getIndex(), hit.getId(), hit.getScore()));
        }
        return new GroundTruth(List.copyOf(neighbors));
    }
}
