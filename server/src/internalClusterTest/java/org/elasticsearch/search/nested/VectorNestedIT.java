/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.nested;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class VectorNestedIT extends ESIntegTestCase {

    public void testSimpleNested() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("vector")
                    .field("type", "dense_vector")
                    .field("index", true)
                    .field("dims", 3)
                    .field("similarity", "cosine")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1))
        );
        ensureGreen();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startArray("nested")
                    .startObject()
                    .field("vector", new float[] { 1, 1, 1 })
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        waitForRelocation(ClusterHealthStatus.GREEN);
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsBytesRef(), notNullValue());
        refresh();

        assertResponse(
            prepareSearch("test").setKnnSearch(
                List.of(
                    new KnnSearchBuilder("nested.vector", new float[] { 1, 1, 1 }, 1, 1, 10f, null, null, null).innerHit(
                        new InnerHitBuilder()
                    )
                )
            ).setAllowPartialSearchResults(false),
            response -> assertThat(response.getHits().getHits().length, greaterThan(0))
        );
    }

    public void testNestedKNnnSearch() {
        testNestedWithTwoSegments(false);
    }

    public void testNestedKNnnSearchWithMultipleSegments() {
        testNestedWithTwoSegments(true);
    }

    private void testNestedWithTwoSegments(boolean flush) {
        assertAcked(prepareCreate("test").setMapping("""
            {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "nested": {
                  "type": "nested",
                  "properties": {
                    "paragraph_id": {
                      "type": "keyword"
                    },
                    "vector": {
                      "type": "dense_vector",
                      "dims": 5,
                      "similarity": "l2_norm",
                      "index_options": {
                        "type": "hnsw"
                      }
                    }
                  }
                }
              }
            }
            """).setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)));
        ensureGreen();

        prepareIndex("test").setId("1")
            .setSource(
                "name",
                "dog",
                "nested",
                new Object[] {
                    Map.of("paragraph_id", 0, "vector", new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f }),
                    Map.of("paragraph_id", 1, "vector", new float[] { 240.0f, 300f, -3f, 1f, -20f }) }
            )
            .get();

        prepareIndex("test").setId("2")
            .setSource(
                "name",
                "cat",
                "nested",
                new Object[] {
                    Map.of("paragraph_id", 0, "vector", new float[] { -0.5f, 100.0f, -13f, 14.8f, -156.0f }),
                    Map.of("paragraph_id", 1, "vector", new float[] { 0f, 100.0f, 0f, 14.8f, -156.0f }),
                    Map.of("paragraph_id", 2, "vector", new float[] { 0f, 1.0f, 0f, 1.8f, -15.0f }) }
            )
            .get();

        if (flush) {
            refresh("test");
        }

        prepareIndex("test").setId("3")
            .setSource(
                "name",
                "rat",
                "nested",
                new Object[] { Map.of("paragraph_id", 0, "vector", new float[] { 0.5f, 111.3f, -13.0f, 14.8f, -156.0f }) }
            )
            .get();

        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();

        var knn = new KnnSearchBuilder("nested.vector", new float[] { -0.5f, 90.0f, -10f, 14.8f, -156.0f }, 2, 3, 10f, null, null, null);
        var request = prepareSearch("test").addFetchField("name").setKnnSearch(List.of(knn));
        assertNoFailuresAndResponse(request, response -> {
            assertHitCount(response, 2);
            assertEquals("2", response.getHits().getHits()[0].getId());
            assertEquals("cat", response.getHits().getHits()[0].field("name").getValue());
            assertEquals("3", response.getHits().getHits()[1].getId());
            assertEquals("rat", response.getHits().getHits()[1].field("name").getValue());
        });
    }
}
