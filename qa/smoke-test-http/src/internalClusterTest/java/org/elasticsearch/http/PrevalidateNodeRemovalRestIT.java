/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.PrevalidateNodeRemovalResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PrevalidateNodeRemovalRestIT extends HttpSmokeTestCase {

    public void testRestStatusCode() throws IOException {
        String node1Name = internalCluster().getRandomNodeName();
        String node1Id = getNodeId(node1Name);
        ensureGreen();
        RestClient client = getRestClient();

        int i = randomIntBetween(0, 2);
        final Request req = switch (i) {
            case 0 -> new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal?names=" + node1Name);
            case 1 -> new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal?ids=" + node1Id);
            case 2 -> new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal?external_ids=" + node1Name);
            default -> throw new IllegalStateException("unexpected value " + i);
        };
        Response response = client.performRequest(req);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        PrevalidateNodeRemovalResponse prevalidationResp;
        try (
            var input = response.getEntity().getContent();
            var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, input)
        ) {
            prevalidationResp = PrevalidateNodeRemovalResponse.fromXContent(parser);
        }
        assertTrue(prevalidationResp.getPrevalidation().isSafe());
        assertThat(prevalidationResp.getPrevalidation().message(), equalTo("cluster status is not RED"));

        try {
            String queryParam = randomFrom("names", "ids", "external_ids") + "=nonExistingNode";
            client.performRequest(new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal?" + queryParam));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp = e.getResponse();
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(404));
        }

        DiscoveryNode node1 = internalCluster().clusterService(node1Name).localNode();
        // if the provides name/ids do not uniquely identify nodes reply with a 404
        try {
            String nodes = String.join(",", "noId", node1.getId());
            client.performRequest(new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal?ids=" + nodes));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp = e.getResponse();
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(404));
        }

        // Using more than one of the query parameters returns 400
        try {
            client.performRequest(
                new Request(
                    HttpPost.METHOD_NAME,
                    Strings.format("/_internal/prevalidate_node_removal?names=%s&ids=%s", node1Name, node1.getId())
                )
            );
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp = e.getResponse();
            assertTrue(
                EntityUtils.toString(resp.getEntity()).contains(PrevalidateNodeRemovalRequest.VALIDATION_ERROR_MSG_ONLY_ONE_QUERY_PARAM)
            );
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(400));
        }

        // Specifying no node returns 400
        try {
            client.performRequest(new Request(HttpPost.METHOD_NAME, "/_internal/prevalidate_node_removal"));
            fail("request should have failed");
        } catch (ResponseException e) {
            Response resp = e.getResponse();
            assertTrue(EntityUtils.toString(resp.getEntity()).contains(PrevalidateNodeRemovalRequest.VALIDATION_ERROR_MSG_NO_QUERY_PARAM));
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(400));
        }
    }
}
