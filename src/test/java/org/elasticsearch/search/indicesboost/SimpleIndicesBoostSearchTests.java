/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleIndicesBoostSearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void testIndicesBoost() throws Exception {
        createIndex("test1", "test2");
        ensureGreen();
        client().index(indexRequest("test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client().index(indexRequest("test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        refresh();

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

    @Test
    public void testIndicesBoostWithAlias() throws Exception {
        createIndex("alias_test1", "alias_test2");
        ensureGreen();
        client().admin().indices().prepareAliases().addAlias("alias_test1", "alias1").get();
        client().admin().indices().prepareAliases().addAlias("alias_test2", "alias2").get();

        client().index(indexRequest("alias_test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client().index(indexRequest("alias_test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        refresh();

        float indexBoost = 1.1f;

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("Query with alias_test1 boosted");
        SearchResponse response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("alias_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("alias_test2"));

        logger.info("Query with alias_test2 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("alias_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("alias_test1"));

        logger.info("--- DFS_QUERY_THEN_FETCH");

        logger.info("Query with alias_test1 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("alias_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("alias_test2"));

        logger.info("Query with alias_test2 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("alias_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("alias_test1"));
    }

    @Test
    public void testIndicesBoostWithWildcard() throws Exception {
        createIndex("wildcard_test1", "wildcard_test2");
        ensureGreen();

        client().index(indexRequest("wildcard_test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client().index(indexRequest("wildcard_test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        refresh();

        float indexBoost = 1.1f;

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("Query with wildcard_test1 boosted");
        SearchResponse response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("*test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("wildcard_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("wildcard_test2"));

        logger.info("Query with wildcard_test2 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("*test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("wildcard_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("wildcard_test1"));

        logger.info("--- DFS_QUERY_THEN_FETCH");

        logger.info("Query with test1 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("*test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("wildcard_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("wildcard_test2"));

        logger.info("Query with wildcard_test2 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("*test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("wildcard_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("wildcard_test1"));
    }

    @Test
    public void testMultipleIndicesBoost() throws Exception {
        createIndex("multi_test1", "multi_test2");
        ensureGreen();
        client().admin().indices().prepareAliases().addAlias("multi_test1", "alias1").get();

        client().index(indexRequest("multi_test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client().index(indexRequest("multi_test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        refresh();

        // Higher boost value is used.
        float indexBoost1 = 1.1f;
        float indexBoost2 = 0.9f;

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("Query with multi_test1 de-boosted");
        SearchResponse response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost1).indexBoost("multi_test1", indexBoost2).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("multi_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("multi_test1"));

        logger.info("Query with multi_test1 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost2).indexBoost("multi_test1", indexBoost1).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("multi_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("multi_test2"));

        logger.info("--- DFS_QUERY_THEN_FETCH");

        logger.info("Query with multi_test1 de-boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost1).indexBoost("multi_test1", indexBoost2).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("multi_test2"));
        assertThat(response.getHits().getAt(1).index(), equalTo("multi_test1"));

        logger.info("Query with multi_test1 boosted");
        response = client().search(searchRequest()
                        .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .source(searchSource().explain(true).indexBoost("alias1", indexBoost2).indexBoost("multi_test1", indexBoost1).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).index(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).index(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).index(), equalTo("multi_test1"));
        assertThat(response.getHits().getAt(1).index(), equalTo("multi_test2"));
    }
}
