/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import joptsimple.internal.Strings;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PrevalidateNodeRemovalRestIT extends HttpSmokeTestCase {

    public void testRestStatusCode() throws IOException {
        String node1 = internalCluster().getRandomNodeName();
        ensureGreen();
        RestClient client = getRestClient();

        final Request req1 = new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal/" + node1);
        Response resp1 = client.performRequest(req1);
        assertThat(resp1.getStatusLine().getStatusCode(), equalTo(200));

        final Request req2 = new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal/nonExistingNode");
        try {
            client.performRequest(req2);
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp2 = e.getResponse();
            assertThat(resp2.getStatusLine().getStatusCode(), equalTo(404));
        }

        // if the provides name/ids do not uniquely identify nodes reply with a Bad Request
        // TODO: is this too strict?
        String nodes = Strings.join(new String[] { node1, internalCluster().clusterService(node1).localNode().getId() }, ",");
        final Request req3 = new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal/" + nodes);
        try {
            client.performRequest(req3);
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp3 = e.getResponse();
            assertThat(resp3.getStatusLine().getStatusCode(), equalTo(400));
        }
    }
}
