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

package org.elasticsearch.test.integration.search.customscore;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class CustomScoreSearchTests extends AbstractNodesTests {

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

    @Test public void testCustomScriptBoost() throws Exception {
        // execute a search before we create an index
        try {
            client.prepareSearch().setQuery(termQuery("test", "value")).execute().actionGet();
            assert false : "should fail";
        } catch (Exception e) {
            // ignore, no indices
        }

        try {
            client.prepareSearch("test").setQuery(termQuery("test", "value")).execute().actionGet();
            assert false : "should fail";
        } catch (Exception e) {
            // ignore, no indices
        }

        client.admin().indices().create(createIndexRequest("test")).actionGet();
        client.index(indexRequest("test").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())).actionGet();
        client.index(indexRequest("test").type("type1").id("2")
                .source(jsonBuilder().startObject().field("test", "value check").field("num1", 2.0f).endObject())).actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("running doc['num1'].value");
        SearchResponse response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running -doc['num1'].value");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("-doc['num1'].value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));


        logger.info("running pow(doc['num1'].value, 2)");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("pow(doc['num1'].value, 2)")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running max(doc['num1'].value, 1)");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("max(doc['num1'].value, 1d)")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running doc['num1'].value * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value * _score")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running param1 * param2 * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("param1 * param2 * _score").param("param1", 2).param("param2", 2)))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
    }
}