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

package org.elasticsearch.test.integration.client.transport;

import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.transport.TransportAddress;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleSingleTransportClientTests extends AbstractServersTests {

    private TransportClient client;

    @AfterMethod public void closeServers() {
        closeAllServers();
        if (client != null) {
            client.close();
        }
    }

    @Test public void testOnlyWithTransportAddress() throws Exception {
        startServer("server1");
        TransportAddress server1Address = ((InternalServer) server("server1")).injector().getInstance(TransportService.class).boundAddress().publishAddress();
        client = new TransportClient(ImmutableSettings.settingsBuilder().putBoolean("discovery.enabled", false).build());
        client.addTransportAddress(server1Address);
        testSimpleActions(client);
    }

    /*@Test*/

    public void testWithDiscovery() throws Exception {
        startServer("server1");
        client = new TransportClient(ImmutableSettings.settingsBuilder().putBoolean("discovery.enabled", true).build());
        // wait a bit so nodes will be discovered
        Thread.sleep(1000);
        testSimpleActions(client);
    }

    private void testSimpleActions(Client client) {
        IndexResponse indexResponse = client.index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.id(), equalTo("1"));
        assertThat(indexResponse.type(), equalTo("type1"));
        RefreshResponse refreshResult = client.admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResult.index("test").successfulShards(), equalTo(5));
        assertThat(refreshResult.index("test").failedShards(), equalTo(0));

        IndicesStatusResponse indicesStatusResponse = client.admin().indices().status(indicesStatus()).actionGet();
        assertThat(indicesStatusResponse.indices().size(), equalTo(1));
        assertThat(indicesStatusResponse.index("test").shards().size(), equalTo(5)); // 5 index shards (1 with 1 backup)
        assertThat(indicesStatusResponse.index("test").docs().numDocs(), equalTo(1));

        GetResponse getResult;

        for (int i = 0; i < 5; i++) {
            getResult = client.get(getRequest("test").type("type1").id("1").threadedOperation(false)).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client.get(getRequest("test").type("type1").id("1").threadedOperation(true)).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
        }

        for (int i = 0; i < 5; i++) {
            getResult = client.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat(getResult.empty(), equalTo(true));
        }

        DeleteResponse deleteResponse = client.delete(deleteRequest("test").type("type1").id("1")).actionGet();
        assertThat(deleteResponse.id(), equalTo("1"));
        assertThat(deleteResponse.type(), equalTo("type1"));
        client.admin().indices().refresh(refreshRequest("test")).actionGet();

        for (int i = 0; i < 5; i++) {
            getResult = client.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.empty(), equalTo(true));
        }

        client.index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        client.index(Requests.indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();

        FlushResponse flushResult = client.admin().indices().flush(flushRequest("test")).actionGet();
        assertThat(flushResult.index("test").successfulShards(), equalTo(5));
        assertThat(flushResult.index("test").failedShards(), equalTo(0));
        client.admin().indices().refresh(refreshRequest("test")).actionGet();

        for (int i = 0; i < 5; i++) {
            getResult = client.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("2", "test")));
        }

        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            CountResponse countResponse = client.count(countRequest("test").querySource(termQuery("_type", "type1"))).actionGet();
            assertThat(countResponse.count(), equalTo(2l));
            assertThat(countResponse.successfulShards(), equalTo(5));
            assertThat(countResponse.failedShards(), equalTo(0));
            // test failed (simply query that can't be parsed)
            countResponse = client.count(countRequest("test").querySource("{ term : { _type : \"type1 } }")).actionGet();

            assertThat(countResponse.count(), equalTo(0l));
            assertThat(countResponse.successfulShards(), equalTo(0));
            assertThat(countResponse.failedShards(), equalTo(5));
        }

        DeleteByQueryResponse queryResponse = client.deleteByQuery(deleteByQueryRequest("test").querySource(termQuery("name", "test2"))).actionGet();
        assertThat(queryResponse.index("test").successfulShards(), equalTo(5));
        assertThat(queryResponse.index("test").failedShards(), equalTo(0));
        client.admin().indices().refresh(refreshRequest("test")).actionGet();

        for (int i = 0; i < 5; i++) {
            getResult = client.get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.source(), equalTo(source("1", "test")));
            getResult = client.get(getRequest("test").type("type1").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.empty(), equalTo(false));
        }


        // stop the server
        closeServer("server1");

        // it should try and reconnect
        try {
            client.index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
            assert false : "should fail...";
        } catch (ConnectTransportException e) {
            // all is well
        }
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
