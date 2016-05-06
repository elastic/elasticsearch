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
import org.apache.lucene.util.LuceneTestCase;

import java.util.logging.LogManager;

public class StaticConnectionPoolTests extends LuceneTestCase {

    static {
        LogManager.getLogManager().reset();
    }

    public void testConstructor() {
        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200);
        }

        try {
            new StaticConnectionPool((HttpHost) null);
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "host cannot be null");
        }

        try {
            new StaticConnectionPool((HttpHost[])null);
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        try {
            new StaticConnectionPool();
        } catch(IllegalArgumentException e) {
            assertEquals(e.getMessage(), "no hosts provided");
        }

        StaticConnectionPool staticConnectionPool = new StaticConnectionPool(hosts);
        assertNotNull(staticConnectionPool);
    }
}
