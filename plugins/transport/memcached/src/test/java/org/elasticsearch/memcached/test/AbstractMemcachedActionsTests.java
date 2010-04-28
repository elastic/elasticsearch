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
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractMemcachedActionsTests {

    private Node node;

    private MemcachedClient memcachedClient;

    @BeforeMethod
    public void setup() throws IOException {
        node = nodeBuilder().node();
        memcachedClient = createMemcachedClient();
    }

    protected abstract MemcachedClient createMemcachedClient() throws IOException;

    @AfterMethod
    public void tearDown() {
        memcachedClient.shutdown();
        node.close();
    }

    @Test public void testSimpleOperations() throws Exception {
        Future setResult = memcachedClient.set("/test/person/1", 0, jsonBuilder().startObject().field("test", "value").endObject().copiedBytes());
        setResult.get(10, TimeUnit.SECONDS);

        String getResult = (String) memcachedClient.get("/_refresh");
        System.out.println("REFRESH " + getResult);

        getResult = (String) memcachedClient.get("/test/person/1");
        System.out.println("GET " + getResult);

        Future deleteResult = memcachedClient.delete("/test/person/1");
        deleteResult.get(10, TimeUnit.SECONDS);

        getResult = (String) memcachedClient.get("/_refresh");
        System.out.println("REFRESH " + getResult);

        getResult = (String) memcachedClient.get("/test/person/1");
        System.out.println("GET " + getResult);
    }
}
