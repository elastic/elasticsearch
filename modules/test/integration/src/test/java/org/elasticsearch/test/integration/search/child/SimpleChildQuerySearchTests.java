/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.child;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.search.facet.FacetBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleChildQuerySearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void simpleChildQuery() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client.prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client.prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client.prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // TEST FETCHING _parent from child
        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(idsQuery("child").ids("c1"))
                .addFields("_parent")
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.hits().getAt(0).field("_parent").value().toString(), equalTo("p1"));

        // TEST matching on parent
        searchResponse = client.prepareSearch("test")
                .setQuery(termQuery("child._parent", "p1"))
                .addFields("_parent")
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.hits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.hits().getAt(1).field("_parent").value().toString(), equalTo("p1"));


        // TOP CHILDREN QUERY

        searchResponse = client.prepareSearch("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test public void simpleChildQueryWithFlush() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data with flushes, so we have many segments
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test public void simpleChildQueryWithFlushAnd3Shards() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data with flushes, so we have many segments
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p2"));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test public void testScopedFacet() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client.prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client.prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client.prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(topChildrenQuery("child", boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))).scope("child1"))
                .addFacet(termsFacet("facet1").field("c_field").scope("child1"))
                .execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
        assertThat(searchResponse.hits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.hits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        assertThat(searchResponse.facets().facets().size(), equalTo(1));
        TermsFacet termsFacet = searchResponse.facets().facet("facet1");
        assertThat(termsFacet.entries().size(), equalTo(2));
        assertThat(termsFacet.entries().get(0).term(), equalTo("red"));
        assertThat(termsFacet.entries().get(0).count(), equalTo(2));
        assertThat(termsFacet.entries().get(1).term(), equalTo("yellow"));
        assertThat(termsFacet.entries().get(1).count(), equalTo(1));
    }

    @Test public void testDeletedParent() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        client.admin().indices().preparePutMapping("test").setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client.prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client.prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client.prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client.prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.hits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.hits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        // update p1 and see what that we get updated values...

        client.prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1_updated").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        searchResponse = client.prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.hits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));

        searchResponse = client.prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        if (searchResponse.failedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.shardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(1l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.hits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));
    }
}
