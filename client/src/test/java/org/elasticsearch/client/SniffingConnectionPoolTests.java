/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.client;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;

import java.util.logging.LogManager;

public class SniffingConnectionPoolTests extends LuceneTestCase {

    static {
        LogManager.getLogManager().reset();
    }

    public void testConstructor() throws Exception {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        Node[] nodes = new Node[numNodes];
        for (int i = 0; i < numNodes; i++) {
            nodes[i] = new Node(new HttpHost("localhost", 9200));
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {

            fail("pool creation should have failed " + connectionPool);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffInterval must be greater than 0");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffAfterFailureDelay must be greater than 0");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), null, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "client cannot be null");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, null,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "sniffRequestConfig cannot be null");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffRequestTimeout must be greater than 0");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), null, nodes)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "connection selector predicate cannot be null");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()),
                connection -> random().nextBoolean(), (Node[])null)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no nodes provided");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(),
                (Node)null)) {
            fail("pool creation should have failed " + connectionPool);
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "node cannot be null");
        }

        try (SniffingConnectionPool connectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean())) {
            fail("pool creation should have failed " + connectionPool);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no nodes provided");
        }

        try (SniffingConnectionPool sniffingConnectionPool = new SniffingConnectionPool(
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), random().nextBoolean(),
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE), httpClient, RequestConfig.DEFAULT,
                RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE),
                RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values()), connection -> random().nextBoolean(), nodes)) {
            assertNotNull(sniffingConnectionPool);
        }
    }
}
