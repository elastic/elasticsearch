/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class KnnVectorSearchIT extends ESIntegTestCase {

    public void testIndexQueryVectorBuilder() {
        float[] storedVector = new float[] { 0, 1, 2, 3 };
        client().admin().indices().prepareCreate("stored_vector").setMapping("""
            {
            "properties": {
            "stored_vector": { "enabled": false }
            }
            }
            """).get();
        client().prepareIndex("stored_vector")
            .setId("vector_0")
            .setSource("stored_vector", storedVector)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin().indices().prepareCreate("knn_index").setMapping("""
            {
            "properties": {
            "vector": {
            "type": "dense_vector",
            "dims": 4,
            "index": true,
            "similarity": "cosine"
            }
            }
            }
            """).get();
        BulkResponse bulkResponse = client().prepareBulk("knn_index")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(client().prepareIndex().setId("0").setSource("vector", storedVector))
            .add(client().prepareIndex().setId("1").setSource("vector", new float[] { 1, 1, 1, 1 }))
            .add(client().prepareIndex().setId("2").setSource("vector", new float[] { 2, 2, 2, 2 }))
            .add(client().prepareIndex().setId("3").setSource("vector", new float[] { 3, 3, 3, 3 }))
            .get();
        assertFalse(bulkResponse.hasFailures());

        SearchResponse response = client().prepareSearch("knn_index")
            .setKnnSearch(
                List.of(
                    new KnnSearchBuilder("vector", new IndexedQueryVectorBuilder("stored_vector", "vector_0", "stored_vector", null), 5, 5)
                )
            )
            .get();
        assertThat(response.getHits().getTotalHits().value, equalTo(4L));
        assertThat(response.getHits().getHits()[0].getId(), equalTo("0"));
    }

}
