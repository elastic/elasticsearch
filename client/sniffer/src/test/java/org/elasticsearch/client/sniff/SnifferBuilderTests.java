/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.sniff;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SnifferBuilderTests extends RestClientTestCase {

    public void testBuild() throws Exception {
        int numNodes = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        HttpHost[] hosts = new HttpHost[numNodes];
        for (int i = 0; i < numNodes; i++) {
            hosts[i] = new HttpHost("localhost", 9200 + i);
        }

        try (RestClient client = RestClient.builder(hosts).build()) {
            try {
                Sniffer.builder(null).build();
                fail("should have failed");
            } catch(NullPointerException e) {
                assertEquals("restClient cannot be null", e.getMessage());
            }

            try {
                Sniffer.builder(client).setSniffIntervalMillis(RandomNumbers.randomIntBetween(getRandom(), Integer.MIN_VALUE, 0));
                fail("should have failed");
            } catch(IllegalArgumentException e) {
                assertEquals("sniffIntervalMillis must be greater than 0", e.getMessage());
            }

            try {
                Sniffer.builder(client).setSniffAfterFailureDelayMillis(RandomNumbers.randomIntBetween(getRandom(), Integer.MIN_VALUE, 0));
                fail("should have failed");
            } catch(IllegalArgumentException e) {
                assertEquals("sniffAfterFailureDelayMillis must be greater than 0", e.getMessage());
            }


            try {
                Sniffer.builder(client).setNodesSniffer(null);
                fail("should have failed");
            } catch(NullPointerException e) {
                assertEquals("nodesSniffer cannot be null", e.getMessage());
            }


            try (Sniffer sniffer = Sniffer.builder(client).build()) {
                assertNotNull(sniffer);
            }

            SnifferBuilder builder = Sniffer.builder(client);
            if (getRandom().nextBoolean()) {
                builder.setSniffIntervalMillis(RandomNumbers.randomIntBetween(getRandom(), 1, Integer.MAX_VALUE));
            }
            if (getRandom().nextBoolean()) {
                builder.setSniffAfterFailureDelayMillis(RandomNumbers.randomIntBetween(getRandom(), 1, Integer.MAX_VALUE));
            }
            if (getRandom().nextBoolean()) {
                builder.setNodesSniffer(new MockNodesSniffer());
            }

            try (Sniffer sniffer = builder.build()) {
                assertNotNull(sniffer);
            }
        }
    }
}
