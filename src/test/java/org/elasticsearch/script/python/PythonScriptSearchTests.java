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

package org.elasticsearch.script.python;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.scriptFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class PythonScriptSearchTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    private Client client;

    @BeforeMethod
    public void createNodes() throws Exception {
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")
                .put("number_of_shards", 1)).node();
        client = node.client();
    }

    @AfterMethod
    public void closeNodes() {
        client.close();
        node.close();
    }

    @Test
    public void testPythonFilter() throws Exception {
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running doc['num1'].value > 1");
        SearchResponse response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > 1").lang("python")))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "python", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("python").addParam("param1", 2)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "python", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("python").addParam("param1", -1)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "python", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.getHits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.getHits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.getHits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.getHits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
    }

    @Test
    public void testScriptFieldUsingSource() throws Exception {
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .startObject("obj1").field("test", "something").endObject()
                        .startObject("obj2").startArray("arr2").value("arr_value1").value("arr_value2").endArray().endObject()
                        .endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        SearchResponse response = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addField("_source.obj1") // we also automatically detect _source in fields
                .addScriptField("s_obj1", "python", "_source['obj1']", null)
                .addScriptField("s_obj1_test", "python", "_source['obj1']['test']", null)
                .addScriptField("s_obj2", "python", "_source['obj2']", null)
                .addScriptField("s_obj2_arr2", "python", "_source['obj2']['arr2']", null)
                .execute().actionGet();

        Map<String, Object> sObj1 = (Map<String, Object>) response.getHits().getAt(0).field("_source.obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.getHits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        sObj1 = (Map<String, Object>) response.getHits().getAt(0).field("s_obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.getHits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        Map<String, Object> sObj2 = (Map<String, Object>) response.getHits().getAt(0).field("s_obj2").value();
        List sObj2Arr2 = (List) sObj2.get("arr2");
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        sObj2Arr2 = (List) response.getHits().getAt(0).field("s_obj2_arr2").value();
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));
    }

    @Test
    public void testCustomScriptBoost() throws Exception {
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
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value").lang("python")))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running -doc['num1'].value");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("-doc['num1'].value").lang("python")))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
        assertThat(response.getHits().getAt(1).id(), equalTo("2"));


        logger.info("running doc['num1'].value * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value * _score").lang("python")))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
        assertThat(response.getHits().getAt(0).id(), equalTo("2"));
        assertThat(response.getHits().getAt(1).id(), equalTo("1"));

        logger.info("running param1 * param2 * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("param1 * param2 * _score").param("param1", 2).param("param2", 2).lang("python")))
        ).actionGet();

        assertThat("Failures " + Arrays.toString(response.getShardFailures()), response.getShardFailures().length, equalTo(0));

        assertThat(response.getHits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.getHits().getAt(0).id(), response.getHits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.getHits().getAt(1).id(), response.getHits().getAt(1).explanation());
    }
}
