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
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.PutFollowResponse;
import org.elasticsearch.client.ccr.UnfollowRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CCRDocumentationIT extends ESRestHighLevelClientTestCase {

    @Before
    public void setupRemoteClusterConfig() throws IOException {
        RestHighLevelClient client = highLevelClient();
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

    public void testPutFollow() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // Create leader index:
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
            createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        // tag::ccr-put-follow-request
        PutFollowRequest putFollowRequest = new PutFollowRequest(
            "local", // <1>
            "leader", // <2>
            "follower" // <3>
        );
        // end::ccr-put-follow-request

        // tag::ccr-put-follow-execute
        PutFollowResponse putFollowResponse =
            client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
        // end::ccr-put-follow-execute

        // tag::ccr-put-follow-response
        boolean isFollowIndexCreated =
            putFollowResponse.isFollowIndexCreated(); // <1>
        boolean isFollowIndexShardsAcked =
            putFollowResponse.isFollowIndexShardsAcked(); // <2>
        boolean isIndexFollowingStarted =
            putFollowResponse.isIndexFollowingStarted(); // <3>
        // end::ccr-put-follow-response

        // Pause following and delete follower index, so that we can execute put follow api again:
        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
            AcknowledgedResponse pauseFollowResponse =  client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseFollowResponse.isAcknowledged(), is(true));

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("follower");
            assertThat(client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged(), is(true));
        }

        // tag::ccr-put-follow-execute-listener
        ActionListener<PutFollowResponse> listener =
            new ActionListener<PutFollowResponse>() {
                @Override
                public void onResponse(PutFollowResponse response) { // <1>
                    boolean isFollowIndexCreated =
                        putFollowResponse.isFollowIndexCreated();
                    boolean isFollowIndexShardsAcked =
                        putFollowResponse.isFollowIndexShardsAcked();
                    boolean isIndexFollowingStarted =
                        putFollowResponse.isIndexFollowingStarted();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-put-follow-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-put-follow-execute-async
        client.ccr().putFollowAsync(putFollowRequest,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-put-follow-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
            AcknowledgedResponse pauseFollowResponse =  client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseFollowResponse.isAcknowledged(), is(true));
        }
    }

    public void testPauseFollow() throws Exception {
        RestHighLevelClient client = highLevelClient();
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
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));
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

    public void testUnfollow() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // Create leader index:
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
            createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
        String followIndex = "follower";
        // Follow index, pause and close, so that it can be unfollowed:
        {
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));

            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest(followIndex);
            AcknowledgedResponse unfollowResponse = client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(unfollowResponse.isAcknowledged(), is(true));

            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followIndex);
            assertThat(client.indices().close(closeIndexRequest, RequestOptions.DEFAULT).isAcknowledged(), is(true));
        }

        // tag::ccr-unfollow-request
        UnfollowRequest request = new UnfollowRequest(followIndex);  // <1>
        // end::ccr-unfollow-request

        // tag::ccr-unfollow-execute
        AcknowledgedResponse response =
            client.ccr().unfollow(request, RequestOptions.DEFAULT);
        // end::ccr-unfollow-execute

        // tag::ccr-unfollow-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ccr-unfollow-response

        // Delete, put follow index, pause and close, so that it can be unfollowed again:
        {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(followIndex);
            assertThat(client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT).isAcknowledged(), is(true));

            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));

            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest(followIndex);
            AcknowledgedResponse unfollowResponse = client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(unfollowResponse.isAcknowledged(), is(true));

            CloseIndexRequest closeIndexRequest = new CloseIndexRequest(followIndex);
            assertThat(client.indices().close(closeIndexRequest, RequestOptions.DEFAULT).isAcknowledged(), is(true));
        }

        // tag::ccr-unfollow-execute-listener
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
        // end::ccr-unfollow-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-unfollow-execute-async
        client.ccr()
            .unfollowAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-unfollow-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
