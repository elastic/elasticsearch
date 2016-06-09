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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.RestClient;

public class HostsSnifferBuilderTests extends LuceneTestCase {

    public void testBuild() throws Exception {
        try {
            HostsSniffer.builder(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals(e.getMessage(), "restClient cannot be null");
        }

        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }

        try (RestClient client = RestClient.builder(hosts).build()) {
            try {
                HostsSniffer.builder(client).setScheme(null);
                fail("should have failed");
            } catch(NullPointerException e) {
                assertEquals(e.getMessage(), "scheme cannot be null");
            }

            try {
                HostsSniffer.builder(client).setSniffRequestTimeout(RandomInts.randomIntBetween(random(), Integer.MIN_VALUE, 0));
                fail("should have failed");
            } catch(IllegalArgumentException e) {
                assertEquals(e.getMessage(), "sniffRequestTimeout must be greater than 0");
            }

            HostsSniffer.Builder builder = HostsSniffer.builder(client);
            if (random().nextBoolean()) {
                builder.setScheme(RandomPicks.randomFrom(random(), HostsSniffer.Scheme.values()));
            }
            if (random().nextBoolean()) {
                builder.setSniffRequestTimeout(RandomInts.randomIntBetween(random(), 1, Integer.MAX_VALUE));
            }
            assertNotNull(builder.build());
        }
    }
}
