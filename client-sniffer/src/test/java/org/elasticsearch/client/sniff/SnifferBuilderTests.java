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
import org.apache.http.HttpHost;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SnifferBuilderTests extends LuceneTestCase {

    public void testBuild() throws Exception {
        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }

        HostsSniffer hostsSniffer = new MockHostsSniffer();

        try (RestClient client = RestClient.builder(hosts).build()) {
            try {
                Sniffer.builder(null, hostsSniffer).build();
                fail("should have failed");
            } catch(NullPointerException e) {
                assertEquals("restClient cannot be null", e.getMessage());
            }

            try {
                Sniffer.builder(client, null).build();
                fail("should have failed");
            } catch(NullPointerException e) {
                assertEquals("hostsSniffer cannot be null", e.getMessage());
            }

            try {
                Sniffer.builder(client, hostsSniffer).setSniffIntervalMillis(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
                fail("should have failed");
            } catch(IllegalArgumentException e) {
                assertEquals("sniffIntervalMillis must be greater than 0", e.getMessage());
            }

            try {
                Sniffer.builder(client, hostsSniffer).setSniffAfterFailureDelayMillis(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
                fail("should have failed");
            } catch(IllegalArgumentException e) {
                assertEquals("sniffAfterFailureDelayMillis must be greater than 0", e.getMessage());
            }

            try (Sniffer sniffer = Sniffer.builder(client, hostsSniffer).build()) {
                assertNotNull(sniffer);
            }

            Sniffer.Builder builder = Sniffer.builder(client, hostsSniffer);
            if (random().nextBoolean()) {
                builder.setSniffIntervalMillis(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
            }
            if (random().nextBoolean()) {
                builder.setSniffAfterFailureDelayMillis(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
            }
            if (random().nextBoolean()) {
                builder.setSniffOnFailure(random().nextBoolean());
            }
            try (Sniffer sniffer = builder.build()) {
                assertNotNull(sniffer);
            }
        }
    }

    private static class MockHostsSniffer extends HostsSniffer {
        MockHostsSniffer() {
            super(null, -1, null);
        }

        @Override
        public List<HttpHost> sniffHosts() throws IOException {
            return Collections.singletonList(new HttpHost("localhost", 9200));
        }
    }
}
