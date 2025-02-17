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

}
