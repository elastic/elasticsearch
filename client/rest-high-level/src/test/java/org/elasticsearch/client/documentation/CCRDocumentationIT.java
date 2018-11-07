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

package org.elasticsearch.client.documentation;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CCRDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPauseFollow() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // Configure local cluster as remote cluster:

            // TODO: replace with nodes info highlevel rest client code when it is available:
            final Request request = new Request("GET", "/_nodes");
            Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
            // Select node info of first node (we don't know the node id):
            nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
            String transportAddress = (String) nodesResponse.get("transport_address");

            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.transientSettings(Collections.singletonMap("cluster.remote.local.seeds", transportAddress));
            ClusterUpdateSettingsResponse updateSettingsResponse =
                client.cluster().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
            assertThat(updateSettingsResponse.isAcknowledged(), is(true));
        }
        {
            // Create leader index:
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
            createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
        String followIndex = "follower";
        // Follow index, so that it can be paused:
        {
            // TODO: Replace this with high level rest client code when put follow API is available:
            final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow");
            request.setJsonEntity("{\"remote_cluster\": \"local\", \"leader_index\": \"leader\"}");
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        }

        // tag::ccr-pause-follow-request
        PauseFollowRequest request = new PauseFollowRequest(followIndex);  // <1>
        // end::ccr-pause-follow-request

        // tag::ccr-pause-follow-execute
        AcknowledgedResponse response =
            client.ccr().pauseFollow(request, RequestOptions.DEFAULT);
        // end::ccr-pause-follow-execute

        // tag::ccr-pause-follow-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ccr-pause-follow-response

        // tag::ccr-pause-follow-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                boolean acknowledged = response.isAcknowledged(); // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::ccr-pause-follow-execute-listener

        // Resume follow index, so that it can be paused again:
        {
            // TODO: Replace this with high level rest client code when resume follow API is available:
            final Request req = new Request("POST", "/" + followIndex + "/_ccr/resume_follow");
            req.setJsonEntity("{}");
            Response res = client().performRequest(req);
            assertThat(res.getStatusLine().getStatusCode(), equalTo(200));
        }

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-pause-follow-execute-async
        client.ccr()
            .pauseFollowAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-pause-follow-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
