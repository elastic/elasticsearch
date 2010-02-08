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

package org.elasticsearch.test.integration.datanode;

import org.elasticsearch.action.PrimaryNotStartedActionException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleDataNodesTests extends AbstractServersTests {

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testDataNodes() throws Exception {
        startServer("nonData1", settingsBuilder().putBoolean("node.data", false).build());
        client("nonData1").admin().indices().create(createIndexRequest("test")).actionGet();
        try {
            client("nonData1").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test")).timeout(timeValueSeconds(1))).actionGet();
            assert false : "no allocation should happen";
        } catch (PrimaryNotStartedActionException e) {
            // all is well
        }

        startServer("nonData2", settingsBuilder().putBoolean("node.data", false).build());
        Thread.sleep(500);

        // still no shard should be allocated
        try {
            client("nonData2").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test")).timeout(timeValueSeconds(1))).actionGet();
            assert false : "no allocation should happen";
        } catch (PrimaryNotStartedActionException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        startServer("data1", settingsBuilder().putBoolean("node.data", true).build());
        Thread.sleep(500);

        IndexResponse indexResponse = client("nonData2").index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.id(), equalTo("1"));
        assertThat(indexResponse.type(), equalTo("type1"));
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
