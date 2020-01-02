/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.vectors.Vectors;
import org.elasticsearch.xpack.vectors.query.AnnPQQueryBuilder;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class PQCodecTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Vectors.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testPQKMeans() throws Exception {
        Settings indexSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .put("index.codec", "ProductQuantizationCodec")
            .build();

        createIndex("index", indexSettings);
        client().admin().indices().preparePutMapping("index")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("my_vector")
                .field("type", "dense_vector")
                .field("dims", 4)
                .field("ann", "pq")
                .field("product_quantizers_count", 2)
                .field("kmeans_algorithm", "lloyds")
                .field("kmeans_iters", 5)
                .field("kmeans_sample_fraction", 1.0)
                .endObject()
                .endObject()
                .endObject()
                .endObject())
            .get();

        BulkRequestBuilder requestBuilder =  client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        float[] vector = new float[4];
        for (int i = 1; i <= 1024; i++) {
            for (int dim = 0; dim < 4; dim++) {
                vector[dim] = randomIntBetween(0, 100);
            }
            requestBuilder.add(client().prepareIndex("index").setId(Integer.toString(i)).setSource("my_vector", vector));
        }
        requestBuilder.get();

        GetResponse response = client().prepareGet("index", "1").get();
        assertTrue(response.isExists());

        float[] queryVector = new float[4];
        for (int dim = 0; dim < 4; dim++) {
            queryVector[dim] = randomIntBetween(0, 100);
        }
        SearchResponse searchResponse = client().prepareSearch("index")
            .setQuery(new AnnPQQueryBuilder("my_vector", queryVector)).setSize(5).get();
        assertNoFailures(searchResponse);
    }
}
