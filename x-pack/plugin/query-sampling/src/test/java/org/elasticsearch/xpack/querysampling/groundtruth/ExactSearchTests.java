/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ExactSearchTests extends ESTestCase {

    public void testRequestScansExactlyWhatTheLiveSearchCouldReturn() {
        float[] vector = { 1f, 2f, 3f };
        QueryBuilder brand = QueryBuilders.termQuery("brand", "apple");
        CapturedQuery query = new CapturedQuery(new String[] { "a", "b" }, "vec", vector, 7, 100, 0.5f, 3f, List.of(brand), "q1");

        SearchRequest request = ExactSearch.request(query);

        assertArrayEquals(new String[] { "a", "b" }, request.indices());
        assertThat(request.source().size(), equalTo(7));
        assertThat(request.source().knnSearch().isEmpty(), equalTo(true));
        assertThat(request.source().query(), instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder bool = (BoolQueryBuilder) request.source().query();
        assertThat(bool.must().size(), equalTo(1));
        assertThat(bool.must().get(0), equalTo(new ExactKnnQueryBuilder(new VectorData(vector, null, null), "vec", null, 1f)));
        assertThat(bool.filter(), equalTo(List.of(brand)));
    }

    public void testUnfilteredQueryHasNoFilter() {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        assertThat(((BoolQueryBuilder) ExactSearch.request(query).source().query()).filter().isEmpty(), equalTo(true));
    }

    public void testParseKeepsRankOrder() {
        SearchResponse response = response(hit("idx", "42", 0.9f), hit("idx", "7", 0.8f), hit("other", "42", 0.1f));
        try {
            GroundTruth truth = ExactSearch.parse(response);
            assertThat(
                truth.neighbors(),
                equalTo(
                    List.of(
                        new CapturedSearch.Hit("idx", "42", 0.9f),
                        new CapturedSearch.Hit("idx", "7", 0.8f),
                        new CapturedSearch.Hit("other", "42", 0.1f)
                    )
                )
            );
        } finally {
            response.decRef();
        }
    }

    public void testParseOfAnEmptyResponse() {
        SearchResponse response = response();
        try {
            assertThat(ExactSearch.parse(response).neighbors().isEmpty(), equalTo(true));
        } finally {
            response.decRef();
        }
        assertThat(
            ExactSearch.request(new CapturedQuery(new String[0], "vec", new float[] { 1f }, 1, 1, null, null, List.of(), null))
                .source()
                .aggregations(),
            nullValue()
        );
    }

    private static SearchHit hit(String index, String id, float score) {
        SearchHit hit = SearchHit.unpooled(randomNonNegativeInt(), id);
        hit.score(score);
        hit.shard(new SearchShardTarget("node", new ShardId(index, "_na_", 0), null));
        return hit;
    }

    private static SearchResponse response(SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
        try {
            return SearchResponseUtils.successfulResponse(searchHits);
        } finally {
            searchHits.decRef(); // the response holds its own reference
        }
    }
}
