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

package org.elasticsearch.rest.action;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

public class RestMainActionTests extends ESTestCase {

    public void testHeadResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final boolean available = randomBoolean();
        final RestStatus expectedStatus = available ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        final Version version = Version.CURRENT;
        final Build build = Build.CURRENT;

        final MainResponse mainResponse = new MainResponse(nodeName, version, clusterName, clusterUUID, build, available);
        XContentBuilder builder = JsonXContent.contentBuilder();
        RestRequest restRequest = new FakeRestRequest() {
            @Override
            public Method method() {
                return Method.HEAD;
            }
        };

        BytesRestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertEquals(expectedStatus, response.status());

        // the empty responses are handled in the HTTP layer so we do
        // not assert on them here
    }

    public void testGetResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final boolean available = randomBoolean();
        final RestStatus expectedStatus = available ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE;
        final Version version = Version.CURRENT;
        final Build build = Build.CURRENT;
        final boolean prettyPrint = randomBoolean();

        final MainResponse mainResponse = new MainResponse(nodeName, version, clusterName, clusterUUID, build, available);
        XContentBuilder builder = JsonXContent.contentBuilder();

        Map<String, String> params = new HashMap<>();
        if (prettyPrint == false) {
            params.put("pretty", String.valueOf(prettyPrint));
        }
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        BytesRestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertEquals(expectedStatus, response.status());
        assertThat(response.content().length(), greaterThan(0));

        XContentBuilder responseBuilder = JsonXContent.contentBuilder();
        if (prettyPrint) {
            // do this to mimic what the rest layer does
            responseBuilder.prettyPrint().lfAtEnd();
        }
        mainResponse.toXContent(responseBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference xcontentBytes = responseBuilder.bytes();
        assertEquals(xcontentBytes, response.content());
    }
}
