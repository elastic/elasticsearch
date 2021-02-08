/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.sniff;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SniffOnFailureListenerTests extends RestClientTestCase {

    public void testSetSniffer() throws Exception {
        SniffOnFailureListener listener = new SniffOnFailureListener();

        try {
            listener.onFailure(null);
            fail("should have failed");
        } catch(IllegalStateException e) {
            assertEquals("sniffer was not set, unable to sniff on failure", e.getMessage());
        }

        try {
            listener.setSniffer(null);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("sniffer must not be null", e.getMessage());
        }

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build()) {
            try (Sniffer sniffer = Sniffer.builder(restClient).setNodesSniffer(new MockNodesSniffer()).build()) {
                listener.setSniffer(sniffer);
                try {
                    listener.setSniffer(sniffer);
                    fail("should have failed");
                } catch(IllegalStateException e) {
                    assertEquals("sniffer can only be set once", e.getMessage());
                }
                listener.onFailure(new Node(new HttpHost("localhost", 9200)));
            }
        }
    }
}
