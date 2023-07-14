/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RestMainActionTests extends ESTestCase {

    public void testHeadResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Version version = Version.CURRENT;
        final IndexVersion indexVersion = IndexVersion.current();
        final Build build = Build.current();

        final MainResponse mainResponse = new MainResponse(
            nodeName,
            version,
            indexVersion.luceneVersion().toString(),
            clusterName,
            clusterUUID,
            build
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        RestRequest restRequest = new FakeRestRequest() {
            @Override
            public Method method() {
                return Method.HEAD;
            }
        };

        RestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));

        // the empty responses are handled in the HTTP layer so we do
        // not assert on them here
    }

    public void testGetResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Version version = Version.CURRENT;
        final IndexVersion indexVersion = IndexVersion.current();
        final TransportVersion transportVersion = TransportVersion.current();
        final Build build = Build.current();
        final boolean prettyPrint = randomBoolean();

        final MainResponse mainResponse = new MainResponse(
            nodeName,
            version,
            indexVersion.luceneVersion().toString(),
            clusterName,
            clusterUUID,
            build
        );
        XContentBuilder builder = JsonXContent.contentBuilder();

        Map<String, String> params = new HashMap<>();
        if (prettyPrint == false) {
            params.put("pretty", String.valueOf(prettyPrint));
        }
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        RestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.content().length(), greaterThan(0));

        XContentBuilder responseBuilder = JsonXContent.contentBuilder();
        if (prettyPrint) {
            // do this to mimic what the rest layer does
            responseBuilder.prettyPrint().lfAtEnd();
        }
        mainResponse.toXContent(responseBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference xcontentBytes = BytesReference.bytes(responseBuilder);
        assertEquals(xcontentBytes, response.content());
    }
}
