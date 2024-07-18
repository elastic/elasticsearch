/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class VectorIT extends ESIntegTestCase {

    public void testSimpleFlat() throws Exception {
        assertSameResults("hnsw", "flat");
    }

    public void testSimpleFlatInt8() throws Exception {
        assertSameResults("int8_hnsw", "int8_flat");
    }

    public void testSimpleFlatInt4() throws Exception {
        assertSameResults("int4_hnsw", "int4_flat");
    }

    private void assertSameResults(String indexedType, String flatType) throws IOException {
        String indexName = "test" + indexedType + "vs" + flatType;
        assertAcked(
            prepareCreate(indexName).setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("vector_flat")
                    .field("type", "dense_vector")
                    .field("index", true)
                    .field("dims", 4)
                    .startObject("index_options")
                    .field("type", flatType)
                    .endObject()
                    .field("similarity", "l2_norm")
                    .endObject()
                    .startObject("vector_indexed")
                    .field("type", "dense_vector")
                    .field("index", true)
                    .field("dims", 4)
                    .startObject("index_options")
                    .field("type", indexedType)
                    .endObject()
                    .field("similarity", "l2_norm")
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1))
        );
        ensureGreen();
        for (int i = 1; i <= 50; i++) {
            var source = jsonBuilder().startObject()
                .field("vector_flat", new float[] { i, i, i, i })
                .field("vector_indexed", new float[] { i, i, i, i })
                .endObject();
            prepareIndex("test" + indexedType + "vs" + flatType).setId(Integer.toString(i)).setSource(source).get();
            // Randomly refresh to flush segments
            if (randomBoolean()) {
                refresh();
            }
        }
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        for (int i = 1; i < 5; i++) {
            float[] query = new float[] { i, i, i, i };
            List<String> topHits = new ArrayList<>();
            assertResponse(
                prepareSearch(indexName).setKnnSearch(List.of(new KnnSearchBuilder("vector_indexed", query, 100, 100, null)))
                    .setAllowPartialSearchResults(false),
                response -> {
                    assertThat(response.getHits().getHits().length, greaterThan(0));
                    for (var hit : response.getHits().getHits()) {
                        topHits.add(hit.getId());
                    }
                }
            );
            assertResponse(
                prepareSearch(indexName).setKnnSearch(List.of(new KnnSearchBuilder("vector_flat", query, 100, 100, null)))
                    .setAllowPartialSearchResults(false),
                response -> {
                    assertThat(response.getHits().getHits().length, greaterThan(0));
                    for (int j = 0; j < response.getHits().getHits().length; j++) {
                        assertThat(response.getHits().getHits()[j].getId(), equalTo(topHits.get(j)));
                    }
                }
            );
            assertResponse(
                prepareSearch(indexName).setQuery(new KnnVectorQueryBuilder("vector_indexed", VectorData.fromFloats(query), 100, 100, null))
                    .setAllowPartialSearchResults(false),
                response -> {
                    assertThat(response.getHits().getHits().length, greaterThan(0));
                    for (int j = 0; j < response.getHits().getHits().length; j++) {
                        assertThat(response.getHits().getHits()[j].getId(), equalTo(topHits.get(j)));
                    }
                }
            );
            assertResponse(
                prepareSearch(indexName).setQuery(new KnnVectorQueryBuilder("vector_flat", VectorData.fromFloats(query), 100, 100, null))
                    .setAllowPartialSearchResults(false),
                response -> {
                    assertThat(response.getHits().getHits().length, greaterThan(0));
                    for (int j = 0; j < response.getHits().getHits().length; j++) {
                        assertThat(response.getHits().getHits()[j].getId(), equalTo(topHits.get(j)));
                    }
                }
            );
        }
    }
}
