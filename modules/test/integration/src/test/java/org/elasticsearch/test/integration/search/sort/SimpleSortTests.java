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

package org.elasticsearch.test.integration.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleSortTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testSimpleSorts() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .field("ivalue", 100)
                .field("dvalue", 0.1)
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .field("svalue", "bbb")
                .field("ivalue", 200)
                .field("dvalue", 0.2)
                .endObject()).execute().actionGet();

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SearchSourceBuilder.Order.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("2"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SearchSourceBuilder.Order.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));


        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("ivalue", SearchSourceBuilder.Order.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("2"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("ivalue", SearchSourceBuilder.Order.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("dvalue", SearchSourceBuilder.Order.ASC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("2"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("dvalue", SearchSourceBuilder.Order.DESC)
                .execute().actionGet();

        assertThat(searchResponse.hits().getTotalHits(), equalTo(2l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));
    }

    @Test public void testDocumentsWithNullValue() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "1")
                .field("svalue", "aaa")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "2")
                .nullField("svalue")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("id", "3")
                .field("svalue", "bbb")
                .endObject()).execute().actionGet();


        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SearchSourceBuilder.Order.ASC)
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(2).field("id").value(), equalTo("3"));

        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SearchSourceBuilder.Order.DESC)
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(3l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("3"));
        assertThat((String) searchResponse.hits().getAt(1).field("id").value(), equalTo("1"));
        assertThat((String) searchResponse.hits().getAt(2).field("id").value(), equalTo("2"));

        // a query with docs just with null values
        searchResponse = client.prepareSearch()
                .setQuery(termQuery("id", "2"))
                .addScriptField("id", "doc['id'].value")
                .addSort("svalue", SearchSourceBuilder.Order.DESC)
                .execute().actionGet();

        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
        assertThat((String) searchResponse.hits().getAt(0).field("id").value(), equalTo("2"));
    }
}
