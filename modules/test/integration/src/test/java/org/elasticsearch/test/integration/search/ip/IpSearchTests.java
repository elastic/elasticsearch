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

package org.elasticsearch.test.integration.search.ip;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class IpSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void filterExistsMissingTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 1)).execute().actionGet();

        client.admin().indices().preparePutMapping("test").setType("type1")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("from").field("type", "ip").endObject()
                        .startObject("to").field("type", "ip").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource("from", "192.168.0.5", "to", "192.168.0.10").setRefresh(true).execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(boolQuery().must(rangeQuery("from").lt("192.168.0.7")).must(rangeQuery("to").gt("192.168.0.7")))
                .execute().actionGet();

        assertThat(search.hits().totalHits(), equalTo(1l));
    }
}
