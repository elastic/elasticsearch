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
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RestClientBuilderTests extends RestClientTestCase {

    public void testBuild() throws IOException {
        try {
            RestClient.builder((HttpHost[])null);
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals("no hosts provided", e.getMessage());
        }

        try {
            RestClient.builder();
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals("no hosts provided", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200), null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("host cannot be null", e.getMessage());
        }

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            assertNotNull(restClient);
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200))
                    .setMaxRetryTimeoutMillis(RandomInts.randomIntBetween(getRandom(), Integer.MIN_VALUE, 0));
            fail("should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals("maxRetryTimeoutMillis must be greater than 0", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200)).setDefaultHeaders(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("defaultHeaders must not be null", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200)).setDefaultHeaders(new Header[]{null});
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("default header must not be null", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200)).setFailureListener(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("failureListener must not be null", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200)).setHttpClientConfigCallback(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("httpClientConfigCallback must not be null", e.getMessage());
        }

        try {
            RestClient.builder(new HttpHost("localhost", 9200)).setRequestConfigCallback(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("requestConfigCallback must not be null", e.getMessage());
        }

        int numNodes = RandomInts.randomIntBetween(getRandom(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }
        RestClientBuilder builder = RestClient.builder(hosts);
        if (getRandom().nextBoolean()) {
            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder;
                }
            });
        }
        if (getRandom().nextBoolean()) {
            builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                    return requestConfigBuilder;
                }
            });
        }
        if (getRandom().nextBoolean()) {
            int numHeaders = RandomInts.randomIntBetween(getRandom(), 1, 5);
            Header[] headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                headers[i] = new BasicHeader("header" + i, "value");
            }
            builder.setDefaultHeaders(headers);
        }
        if (getRandom().nextBoolean()) {
            builder.setMaxRetryTimeoutMillis(RandomInts.randomIntBetween(getRandom(), 1, Integer.MAX_VALUE));
        }
        try (RestClient restClient = builder.build()) {
            assertNotNull(restClient);
        }
    }
}
