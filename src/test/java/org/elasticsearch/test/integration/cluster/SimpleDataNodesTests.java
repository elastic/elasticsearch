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

package org.elasticsearch.test.integration.cluster;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleDataNodesTests extends AbstractNodesTests {

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testDataNodes() throws Exception {
        startNode("nonData1", settingsBuilder().put("node.data", false).build());
        client("nonData1").admin().indices().create(createIndexRequest("test")).actionGet();
        try {
            client("nonData1").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test")).timeout(timeValueSeconds(1))).actionGet();
            assert false : "no allocation should happen";
        } catch (UnavailableShardsException e) {
            // all is well
        }

        startNode("nonData2", settingsBuilder().put("node.data", false).build());
        assertThat(client("nonData2").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        // still no shard should be allocated
        try {
            client("nonData2").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test")).timeout(timeValueSeconds(1))).actionGet();
            assert false : "no allocation should happen";
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        startNode("data1", settingsBuilder().put("node.data", true).build());
        assertThat(client("nonData2").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        IndexResponse indexResponse = client("nonData2").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
        assertThat(indexResponse.getType(), equalTo("type1"));
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
