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

package org.elasticsearch.memcached.test;

import net.spy.memcached.MemcachedClient;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.node.Node;
import org.hamcrest.Matchers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractMemcachedActionsTests {

    private Node node;

    private MemcachedClient memcachedClient;

    @BeforeMethod
    public void setup() throws IOException {
        node = nodeBuilder().settings(settingsBuilder()
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")).node();
        memcachedClient = createMemcachedClient();
    }

    protected abstract MemcachedClient createMemcachedClient() throws IOException;

    @AfterMethod
    public void tearDown() {
        memcachedClient.shutdown();
        node.close();
    }

    @Test public void testSimpleOperations() throws Exception {
        // TODO seems to use SetQ, which is not really supported yet
//        List<Future<Boolean>> setResults = Lists.newArrayList();
//
//        for (int i = 0; i < 10; i++) {
//            setResults.add(memcachedClient.set("/test/person/" + i, 0, jsonBuilder().startObject().field("test", "value").endObject().copiedBytes()));
//        }
//
//        for (Future<Boolean> setResult : setResults) {
//            assertThat(setResult.get(10, TimeUnit.SECONDS), equalTo(true));
//        }

        Future<Boolean> setResult = memcachedClient.set("/test/person/1", 0, jsonBuilder().startObject().field("test", "value").endObject().copiedBytes());
        assertThat(setResult.get(10, TimeUnit.SECONDS), equalTo(true));

        ClusterHealthResponse health = node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        String getResult = (String) memcachedClient.get("/_refresh");
        System.out.println("REFRESH " + getResult);
        assertThat(getResult, Matchers.containsString("\"total\":10"));
        assertThat(getResult, Matchers.containsString("\"successful\":5"));
        assertThat(getResult, Matchers.containsString("\"failed\":0"));

        getResult = (String) memcachedClient.get("/test/person/1");
        System.out.println("GET " + getResult);
        assertThat(getResult, Matchers.containsString("\"_index\":\"test\""));
        assertThat(getResult, Matchers.containsString("\"_type\":\"person\""));
        assertThat(getResult, Matchers.containsString("\"_id\":\"1\""));

        Future<Boolean> deleteResult = memcachedClient.delete("/test/person/1");
        assertThat(deleteResult.get(10, TimeUnit.SECONDS), equalTo(true));

        getResult = (String) memcachedClient.get("/_refresh");
        System.out.println("REFRESH " + getResult);
        assertThat(getResult, Matchers.containsString("\"total\":10"));
        assertThat(getResult, Matchers.containsString("\"successful\":5"));
        assertThat(getResult, Matchers.containsString("\"failed\":0"));

        getResult = (String) memcachedClient.get("/test/person/1");
        System.out.println("GET " + getResult);
    }
}
