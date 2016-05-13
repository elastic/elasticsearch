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
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.LogManager;

public class RestClientBuilderTests extends LuceneTestCase {

    static {
        LogManager.getLogManager().reset();
    }

    public void testBuild() throws IOException {
        try {
            RestClient.builder().setMaxRetryTimeout(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "maxRetryTimeout must be greater than 0");
        }

        try {
            RestClient.builder().setHosts((HttpHost[])null);
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            RestClient.builder().setHosts();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            RestClient.builder().build();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            RestClient.builder().setHosts(new HttpHost[]{new HttpHost("localhost", 9200), null}).build();
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "host cannot be null");
        }

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            RestClient.builder().setHttpClient(httpClient)
                    .setDefaultHeaders(Collections.singleton(new BasicHeader("header", "value"))).build();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "defaultHeaders need to be set to the HttpClient directly when manually provided");
        }

        RestClient.Builder builder = RestClient.builder();
        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }
        builder.setHosts(hosts);

        if (random().nextBoolean()) {
            builder.setHttpClient(HttpClientBuilder.create().build());
        } else {
            if (random().nextBoolean()) {
                int numHeaders = RandomInts.randomIntBetween(random(), 1, 5);
                Collection<BasicHeader> headers = new ArrayList<>(numHeaders);
                for (int i = 0; i < numHeaders; i++) {
                    headers.add(new BasicHeader("header" + i, "value"));
                }
                builder.setDefaultHeaders(headers);
            }
        }

        if (random().nextBoolean()) {
            builder.setMaxRetryTimeout(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
        }
        try (RestClient restClient = builder.build()) {
            assertNotNull(restClient);
        }
    }
}
