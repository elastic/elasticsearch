/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.indicesboost;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleIndicesBoostSearchTests extends ElasticsearchIntegrationTest {

    private static final Settings DEFAULT_SETTINGS = ImmutableSettings.settingsBuilder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

    @Test
    public void testIndicesBoost() throws Exception {
        ElasticsearchAssertions.assertHitCount(client().prepareSearch().setQuery(termQuery("test", "value")).get(), 0);

        try {
            client().prepareSearch("test").setQuery(termQuery("test", "value")).execute().actionGet();
            assert false : "should fail";
        } catch (Exception e) {
            // ignore, no indices
        }

        client().admin().indices().create(createIndexRequest("test1").settings(DEFAULT_SETTINGS)).actionGet();
        client().admin().indices().create(createIndexRequest("test2").settings(DEFAULT_SETTINGS)).actionGet();
        client().index(indexRequest("test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client().index(indexRequest("test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        float indexBoost = 1.1f;

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("Query with test1 boosted");
        SearchResponse response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("test2"));

        logger.info("Query with test2 boosted");
        response = client().search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("test1"));

        logger.info("--- DFS_QUERY_THEN_FETCH");

        logger.info("Query with test1 boosted");
        response = client().search(searchRequest()
                .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("test2"));

        logger.info("Query with test2 boosted");
        response = client().search(searchRequest()
                .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("test1"));
    }
}
