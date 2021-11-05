/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.vectors.DenseVectorPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class KnnSearchActionTests extends ESSingleNodeTestCase {
    private static final int VECTOR_DIMENSION = 10;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DenseVectorPlugin.class);
    }

    public void testTotalHits() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index1", indexSettings, builder);
        createIndex("index2", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            client().prepareIndex("index1").setId(String.valueOf(doc)).setSource("vector", randomVector()).get();
            client().prepareIndex("index2").setId(String.valueOf(doc)).setSource("vector", randomVector()).get();
        }

        client().admin().indices().prepareForceMerge("index1", "index2").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("index1", "index2").get();

        // Since there's no kNN search action at the transport layer, we just emulate
        // how the action works (it builds a kNN query under the hood)
        float[] queryVector = randomVector();
        SearchResponse response = client().prepareSearch("index1", "index2")
            .setQuery(new KnnVectorQueryBuilder("vector", queryVector, 5))
            .setSize(2)
            .get();

        // The total hits is num_cands * num_shards, since the query gathers num_cands hits from each shard
        assertHitCount(response, 5 * 2);
        assertEquals(2, response.getHits().getHits().length);
    }

    private float[] randomVector() {
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
