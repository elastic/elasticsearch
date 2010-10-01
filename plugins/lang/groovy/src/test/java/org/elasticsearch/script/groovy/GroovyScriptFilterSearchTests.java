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

package org.elasticsearch.script.groovy;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class GroovyScriptFilterSearchTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    private Client client;

    @BeforeMethod public void createNodes() throws Exception {
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none").put("number_of_shards", 1)).node();
        client = node.client();
    }

    @AfterMethod public void closeNodes() {
        client.close();
        node.close();
    }

    @Test public void testGroovyScriptFilter() throws Exception {
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
                .setQuery(filtered(matchAllQuery(), scriptFilter("doc['num1'].value > 1").lang("groovy")))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "groovy", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.hits().getAt(1).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filtered(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("groovy").addParam("param1", 2)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "groovy", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(1l));
        assertThat(response.hits().getAt(0).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(3.0));

        logger.info("running doc['num1'].value > param1");
        response = client.prepareSearch()
                .setQuery(filtered(matchAllQuery(), scriptFilter("doc['num1'].value > param1").lang("groovy").addParam("param1", -1)))
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "groovy", "doc['num1'].value", null)
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(3l));
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.hits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
    }
}
