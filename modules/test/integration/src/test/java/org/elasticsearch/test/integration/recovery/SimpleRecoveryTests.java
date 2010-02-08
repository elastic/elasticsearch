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

package org.elasticsearch.test.integration.recovery;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleRecoveryTests extends AbstractServersTests {

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testSimpleRecovery() throws Exception {
        startServer("server1");

        client("server1").admin().indices().create(createIndexRequest("test")).actionGet(5000);

        client("server1").index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        client("server1").admin().indices().flush(flushRequest("test")).actionGet();
        client("server1").index(indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();
        client("server1").admin().indices().refresh(refreshRequest("test")).actionGet();

        startServer("server2");
        // sleep so we recover properly
        Thread.sleep(5000);

        GetResponse getResult;

        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1").threadedOperation(false)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("1", "test")));
            getResult = client("server2").get(getRequest("test").type("type1").id("1").threadedOperation(false)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").type("type1").id("2").threadedOperation(true)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("2", "test")));
            getResult = client("server2").get(getRequest("test").type("type1").id("2").threadedOperation(true)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("2", "test")));
        }

        // now start another one so we move some primaries
        startServer("server3");
        Thread.sleep(5000);

        for (int i = 0; i < 5; i++) {
            getResult = client("server1").get(getRequest("test").type("type1").id("1")).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("1", "test")));
            getResult = client("server2").get(getRequest("test").type("type1").id("1")).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("1", "test")));
            getResult = client("server3").get(getRequest("test").type("type1").id("1")).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("1", "test")));
            getResult = client("server1").get(getRequest("test").type("type1").id("2").threadedOperation(true)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("2", "test")));
            getResult = client("server2").get(getRequest("test").type("type1").id("2").threadedOperation(true)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("2", "test")));
            getResult = client("server3").get(getRequest("test").type("type1").id("2").threadedOperation(true)).actionGet(1000);
            assertThat(getResult.source(), equalTo(source("2", "test")));
        }
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
