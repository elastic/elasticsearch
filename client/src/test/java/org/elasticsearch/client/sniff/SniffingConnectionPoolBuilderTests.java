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

package org.elasticsearch.client.sniff;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;

import java.util.Arrays;
import java.util.logging.LogManager;

public class SniffingConnectionPoolBuilderTests extends LuceneTestCase {

    static {
        LogManager.getLogManager().reset();
    }

    public void testBuild() throws Exception {

        try {
            SniffingConnectionPool.builder().setScheme(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "scheme cannot be null");
        }

        try {
            SniffingConnectionPool.builder().setScheme("whatever");
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "scheme must be either http or https");
        }

        try {
            SniffingConnectionPool.builder().setSniffInterval(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffInterval must be greater than 0");
        }

        try {
            SniffingConnectionPool.builder().setSniffRequestTimeout(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffRequestTimeout must be greater than 0");
        }

        try {
            SniffingConnectionPool.builder().setSniffAfterFailureDelay(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "sniffAfterFailureDelay must be greater than 0");
        }

        try {
            SniffingConnectionPool.builder().build();
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "httpClient cannot be null");
        }

        try {
            SniffingConnectionPool.builder().setHttpClient(HttpClientBuilder.create().build()).build();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            SniffingConnectionPool.builder().setHttpClient(HttpClientBuilder.create().build()).setHosts((HttpHost[])null).build();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            SniffingConnectionPool.builder().setHttpClient(HttpClientBuilder.create().build()).setHosts().build();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }

        try (SniffingConnectionPool connectionPool = SniffingConnectionPool.builder()
                .setHttpClient(HttpClientBuilder.create().build()).setHosts(hosts).build()) {
            assertNotNull(connectionPool);
        }

        SniffingConnectionPool.Builder builder = SniffingConnectionPool.builder()
                .setHttpClient(HttpClientBuilder.create().build()).setHosts(hosts);
        if (random().nextBoolean()) {
            builder.setScheme(RandomPicks.randomFrom(random(), Arrays.asList("http", "https")));
        }
        if (random().nextBoolean()) {
            builder.setSniffInterval(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
        }
        if (random().nextBoolean()) {
            builder.setSniffAfterFailureDelay(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
        }
        if (random().nextBoolean()) {
            builder.setSniffRequestTimeout(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
        }
        if (random().nextBoolean()) {
            builder.setSniffOnFailure(random().nextBoolean());
        }
        if (random().nextBoolean()) {
            builder.setSniffRequestConfig(RequestConfig.DEFAULT);
        }
        try (SniffingConnectionPool connectionPool = builder.build()) {
            assertNotNull(connectionPool);
        }
    }
}
