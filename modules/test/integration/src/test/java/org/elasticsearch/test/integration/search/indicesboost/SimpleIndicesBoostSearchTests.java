/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.search.indicesboost;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.elasticsearch.util.json.JsonBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimpleIndicesBoostSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeMethod public void createNodes() throws Exception {
        startNode("server1");
        client = getClient();
    }

    @AfterMethod public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testIndicesBoost() throws Exception {
        client.admin().indices().create(createIndexRequest("test1")).actionGet();
        client.admin().indices().create(createIndexRequest("test2")).actionGet();
        client.index(indexRequest("test1").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value check").endObject())).actionGet();
        client.index(indexRequest("test2").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").endObject())).actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        float indexBoost = 1.1f;

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("Query with test1 boosted");
        SearchResponse response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).index(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).index(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).index(), equalTo("test1"));
        assertThat(response.hits().getAt(1).index(), equalTo("test2"));

        logger.info("Query with test2 boosted");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).index(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).index(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).index(), equalTo("test2"));
        assertThat(response.hits().getAt(1).index(), equalTo("test1"));

        logger.info("--- DFS_QUERY_THEN_FETCH");

        logger.info("Query with test1 boosted");
        response = client.search(searchRequest()
                .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).index(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).index(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).index(), equalTo("test1"));
        assertThat(response.hits().getAt(1).index(), equalTo("test2"));

        logger.info("Query with test2 boosted");
        response = client.search(searchRequest()
                .searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery("test", "value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).index(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).index(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).index(), equalTo("test2"));
        assertThat(response.hits().getAt(1).index(), equalTo("test1"));
    }
}
