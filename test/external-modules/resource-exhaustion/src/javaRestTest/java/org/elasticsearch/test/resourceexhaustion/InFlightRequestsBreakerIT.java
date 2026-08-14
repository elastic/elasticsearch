/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.resourceexhaustion;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the in-flight requests circuit breaker returns 429 when a large request body
 * is received by the coordinating node. Exercises the coordinating path specifically: the
 * coordinating node holds the request body while decoding it before forwarding to the data node,
 * and must reject oversized payloads before they consume unbounded memory.
 *
 * The equivalent single-node coverage exists in Netty4HttpRequestSizeLimitIT (internal cluster
 * test). This class adds the dedicated-coordinating-node variant via the REST test framework.
 */
public class InFlightRequestsBreakerIT extends ResourceExhaustionCoordinatingTestCase {

    public void testLargeRequestBodyTrips429OnCoordinatingNode() throws IOException {
        // Set a low limit so a predictable payload size reliably trips the breaker.
        // Incremental bulk must be disabled: it streams the body in chunks and releases
        // memory as it goes, so the full payload is never in-flight at once.
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity("""
            {
              "persistent": {
                "network.breaker.inflight_requests.limit": "1mb",
                "rest.incremental_bulk": false
              }
            }
            """);
        client().performRequest(settingsRequest);

        try {
            StringBuilder body = new StringBuilder();
            while (body.length() < 2 * 1024 * 1024) {
                body.append("{\"index\":{\"_index\":\"test\"}}\n{\"field\":\"value\"}\n");
            }

            Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.setJsonEntity(body.toString());

            try (RestClient coordClient = coordinatingClient()) {
                ResponseException e = expectThrows(ResponseException.class, () -> coordClient.performRequest(bulkRequest));
                assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(429));
            }
        } finally {
            Request reset = new Request("PUT", "/_cluster/settings");
            reset.setJsonEntity("""
                {
                  "persistent": {
                    "network.breaker.inflight_requests.limit": null,
                    "rest.incremental_bulk": null
                  }
                }
                """);
            client().performRequest(reset);
        }
    }
}
