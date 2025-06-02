/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.nested;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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
                List.of(new KnnSearchBuilder("nested.vector", new float[] { 1, 1, 1 }, 1, 1, null, null).innerHit(new InnerHitBuilder()))
            ).setAllowPartialSearchResults(false),
            response -> assertThat(response.getHits().getHits().length, greaterThan(0))
        );
    }

    public void testSimpleIVFNested() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("name")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("paragraph_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("vector")
                    .field("type", "dense_vector")
                    .field("index", true)
                    .field("dims", 5)
                    .field("similarity", "l2_norm")
                    .startObject("index_options")
                    .field("type", "bbq_ivf")
                    .endObject()
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
                    .field("name", "cow.jpg")
                    .startArray("nested")
                    .startObject()
                    .field("paragraph_id", "0")
                    .field("vector", new float[] { 230, 300.33f, -34.8988f, 15.555f, -200 })
                    .endObject()
                    .startObject()
                    .field("paragraph_id", "1")
                    .field("vector", new float[] { 240, 300, -3, 1, -20 })
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "moose.jpg")
                    .startArray("nested")
                    .startObject()
                    .field("paragraph_id", "0")
                    .field("vector", new float[] { -0.5f, 100, -13, 14.8f, -156 })
                    .endObject()
                    .startObject()
                    .field("paragraph_id", "1")
                    .field("vector", new float[] { 0, 100, 0, 14.8f, -156 })
                    .endObject()
                    .startObject()
                    .field("paragraph_id", "2")
                    .field("vector", new float[] { 0, 1, 0, 1.8f, -15 })
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "rabbit.jpg")
                    .startArray("nested")
                    .startObject()
                    .field("paragraph_id", "0")
                    .field("vector", new float[] { 0.5f, 111.3f, -13f, 14.8f, -156f })
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();
        refresh();

        waitForRelocation(ClusterHealthStatus.GREEN);

        assertResponse(
            prepareSearch("test").setQuery(
                QueryBuilders.nestedQuery(
                    "nested",
                    new KnnVectorQueryBuilder("nested.vector", new float[] { -0.5f, 90, -10, 14.8f, -156 }, null, 3, null, null),
                    ScoreMode.Max
                )
            ).setAllowPartialSearchResults(false),
            response -> {
                assertThat(response.getHits().getHits().length, greaterThan(0));
                assertThat(response.getHits().getHits()[0].getId(), equalTo("2"));
            }
        );
    }

}
