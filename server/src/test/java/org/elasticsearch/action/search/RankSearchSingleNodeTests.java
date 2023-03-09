/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.rank.rrf.RRFRankContextBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class RankSearchSingleNodeTests extends ESSingleNodeTestCase {

    public void testSimpleRRFRank() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("int")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 1000; doc++) {
            client().prepareIndex("index").setSource("vector", new float[] { doc }, "int", doc % 100).get();
        }

        client().admin().indices().prepareRefresh("index").get();

        float[] queryVector = { 500.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 100, 300);
        SearchResponse response = client().prepareSearch("index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(100).rankConstant(1))
            .setTrackTotalHits(true)
            .setKnnSearch(List.of(knnSearch))
            .setQuery(QueryBuilders.rangeQuery("int").lt(2))
            .addFetchField("*")
            .setSize(10)
            .addAggregation(new TermsAggregationBuilder("int-agg").field("int"))
            .get();

        assertEquals(10, response.getHits().getHits().length);
    }

    public void testRRFRankSmallerThanSize() throws IOException {
        int numShards = 1;// + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 1)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        client().prepareIndex("index").setSource("vector", new float[] { 0.0f }, "text", "term term").get();
        client().prepareIndex("index").setSource("vector", new float[] { 1.0f }, "text", "other").get();
        client().prepareIndex("index").setSource("vector", new float[] { 2.0f }, "text", "term").get();

        client().admin().indices().prepareRefresh("index").get();

        float[] queryVector = { 0.0f };
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 3, 3);
        SearchResponse response = client().prepareSearch("index")
            .setRankContextBuilder(new RRFRankContextBuilder().windowSize(100).rankConstant(1))
            .setKnnSearch(List.of(knnSearch))
            .setQuery(QueryBuilders.termQuery("text", "term"))
            .addFetchField("*")
            .get();

        assertEquals(3, response.getHits().getHits().length);
        assertEquals(3, response.getHits().getHits().length);
        // assertEquals(1, response.getHits().getAt(0).getRank());
        if (true) throw new RuntimeException("" + response.getHits().getAt(0).field("vector").getValue().getClass());
        assertEquals(0.0, response.getHits().getAt(0).field("vector").getValue(), 0.0);
        assertEquals("term term", response.getHits().getAt(0).field("text").getValue());
    }
}
