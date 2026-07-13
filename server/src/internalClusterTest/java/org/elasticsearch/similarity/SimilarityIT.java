/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.similarity;

import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimilarityIT extends ESIntegTestCase {
    public void testCustomBM25Similarity() throws Exception {
        try {
            indicesAdmin().prepareDelete("test").get();
        } catch (Exception e) {
            // ignore
        }

        indicesAdmin().prepareCreate("test")
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("similarity", "custom")
                    .field("type", "text")
                    .endObject()
                    .startObject("field2")
                    .field("similarity", "boolean")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .setSettings(
                indexSettings(1, 0).put("similarity.custom.type", "BM25").put("similarity.custom.k1", 2.0f).put("similarity.custom.b", 0.5f)
            )
            .get();

        prepareIndex("test").setId("1")
            .setSource("field1", "the quick brown fox jumped over the lazy dog", "field2", "the quick brown fox jumped over the lazy dog")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        assertResponse(prepareSearch().setQuery(matchQuery("field1", "quick brown fox")), bm25SearchResponse -> {
            assertThat(bm25SearchResponse.getHits().getTotalHits().value(), equalTo(1L));
            float bm25Score = bm25SearchResponse.getHits().getHits()[0].getScore();
            assertResponse(prepareSearch().setQuery(matchQuery("field2", "quick brown fox")), booleanSearchResponse -> {
                assertThat(booleanSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
                float defaultScore = booleanSearchResponse.getHits().getHits()[0].getScore();
                assertThat(bm25Score, not(equalTo(defaultScore)));
            });
        });
    }
}
