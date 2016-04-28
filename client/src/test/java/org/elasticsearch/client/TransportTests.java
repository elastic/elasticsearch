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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.logging.LogManager;
import java.util.stream.Stream;

public class TransportTests extends LuceneTestCase {

    static {
        LogManager.getLogManager().reset();
    }

    public void testConstructor() {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        ConnectionPool<Connection> connectionPool = new ConnectionPool<Connection>() {
            @Override
            public Stream<Connection> nextConnection() {
                return null;
            }

            @Override
            public Connection lastResortConnection() {
                return null;
            }

            @Override
            public void beforeAttempt(Connection connection) throws IOException {

            }

            @Override
            public void onSuccess(Connection connection) {

            }

            @Override
            public void onFailure(Connection connection) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }
        };

        try {
            new Transport<>(null, connectionPool, RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
            fail("transport creation should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "client cannot be null");
        }

        try {
            new Transport<>(httpClient, null, RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
            fail("transport creation should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "connectionPool cannot be null");
        }

        try {
            new Transport<>(httpClient, connectionPool, RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
            fail("transport creation should have failed");
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "maxRetryTimeout must be greater than 0");
        }

        Transport<Connection> transport = new Transport<>(httpClient, connectionPool, RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
        assertNotNull(transport);
    }
}
