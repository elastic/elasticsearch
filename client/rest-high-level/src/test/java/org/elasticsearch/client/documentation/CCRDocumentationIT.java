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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ccr.AutoFollowStats;
import org.elasticsearch.client.ccr.CcrStatsRequest;
import org.elasticsearch.client.ccr.CcrStatsResponse;
import org.elasticsearch.client.ccr.DeleteAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.FollowInfoRequest;
import org.elasticsearch.client.ccr.FollowInfoResponse;
import org.elasticsearch.client.ccr.FollowStatsRequest;
import org.elasticsearch.client.ccr.FollowStatsResponse;
import org.elasticsearch.client.ccr.ForgetFollowerRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.GetAutoFollowPatternResponse;
import org.elasticsearch.client.ccr.GetAutoFollowPatternResponse.Pattern;
import org.elasticsearch.client.ccr.IndicesFollowStats;
import org.elasticsearch.client.ccr.PauseFollowRequest;
import org.elasticsearch.client.ccr.PutAutoFollowPatternRequest;
import org.elasticsearch.client.ccr.PutFollowRequest;
import org.elasticsearch.client.ccr.PutFollowResponse;
import org.elasticsearch.client.ccr.ResumeFollowRequest;
import org.elasticsearch.client.ccr.UnfollowRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.BroadcastResponse;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
            "follower", // <3>
            ActiveShardCount.ONE // <4>
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
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex, ActiveShardCount.ONE);
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
            ResumeFollowRequest resumeFollowRequest = new ResumeFollowRequest(followIndex);
            AcknowledgedResponse resumeResponse = client.ccr().resumeFollow(resumeFollowRequest, RequestOptions.DEFAULT);
            assertThat(resumeResponse.isAcknowledged(), is(true));
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

    public void testResumeFollow() throws Exception {
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
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex, ActiveShardCount.ONE);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));
        }

        // Pause follow index, so that it can be resumed:
        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest(followIndex);
            AcknowledgedResponse pauseResponse = client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-resume-follow-request
        ResumeFollowRequest request = new ResumeFollowRequest(followIndex);  // <1>
        // end::ccr-resume-follow-request

        // tag::ccr-resume-follow-execute
        AcknowledgedResponse response =
            client.ccr().resumeFollow(request, RequestOptions.DEFAULT);
        // end::ccr-resume-follow-execute

        // tag::ccr-resume-follow-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ccr-resume-follow-response

        // Pause follow index, so that it can be resumed again:
        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest(followIndex);
            AcknowledgedResponse pauseResponse = client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-resume-follow-execute-listener
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
        // end::ccr-resume-follow-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-resume-follow-execute-async
        client.ccr()
            .resumeFollowAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-resume-follow-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        // Cleanup:
        client.ccr().pauseFollow(new PauseFollowRequest(followIndex), RequestOptions.DEFAULT);
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
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex, ActiveShardCount.ONE);
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

            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followIndex, ActiveShardCount.ONE);
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

    public void testForgetFollower() throws InterruptedException, IOException {
        final RestHighLevelClient client = highLevelClient();
        final String leaderIndex = "leader";
        {
            // create leader index
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest(leaderIndex);
            final Map<String, String> settings = new HashMap<>(2);
            final int numberOfShards = randomIntBetween(1, 2);
            settings.put("index.number_of_shards", Integer.toString(numberOfShards));
            settings.put("index.soft_deletes.enabled", Boolean.TRUE.toString());
            createIndexRequest.settings(settings);
            final CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
        final String followerIndex = "follower";

        final PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", followerIndex, ActiveShardCount.ONE);
        final PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
        assertTrue(putFollowResponse.isFollowIndexCreated());
        assertTrue((putFollowResponse.isFollowIndexShardsAcked()));
        assertTrue(putFollowResponse.isIndexFollowingStarted());

        final PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
        AcknowledgedResponse pauseFollowResponse = client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
        assertTrue(pauseFollowResponse.isAcknowledged());

        final String followerCluster = highLevelClient().info(RequestOptions.DEFAULT).getClusterName();
        final Request statsRequest = new Request("GET", "/follower/_stats");
        final Response statsResponse = client().performRequest(statsRequest);
        final ObjectPath statsObjectPath = ObjectPath.createFromResponse(statsResponse);
        final String followerIndexUUID = statsObjectPath.evaluate("indices.follower.uuid");

        final String leaderCluster = "local";

        // tag::ccr-forget-follower-request
        final ForgetFollowerRequest request = new ForgetFollowerRequest(
                followerCluster, // <1>
                followerIndex, // <2>
                followerIndexUUID, // <3>
                leaderCluster, // <4>
                leaderIndex); // <5>
        // end::ccr-forget-follower-request

        // tag::ccr-forget-follower-execute
        final BroadcastResponse response = client
                .ccr()
                .forgetFollower(request, RequestOptions.DEFAULT);
        // end::ccr-forget-follower-execute

        // tag::ccr-forget-follower-response
        final BroadcastResponse.Shards shards = response.shards(); // <1>
        final int total = shards.total(); // <2>
        final int successful = shards.successful(); // <3>
        final int skipped = shards.skipped(); // <4>
        final int failed = shards.failed(); // <5>
        shards.failures().forEach(failure -> {}); // <6>
        // end::ccr-forget-follower-response

        // tag::ccr-forget-follower-execute-listener
        ActionListener<BroadcastResponse> listener =
                new ActionListener<BroadcastResponse>() {

                    @Override
                    public void onResponse(final BroadcastResponse response) {
                        final BroadcastResponse.Shards shards = // <1>
                                response.shards();
                        final int total = shards.total();
                        final int successful = shards.successful();
                        final int skipped = shards.skipped();
                        final int failed = shards.failed();
                        shards.failures().forEach(failure -> {});
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        // <2>
                    }

                };
        // end::ccr-forget-follower-execute-listener

        // replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-forget-follower-execute-async
        client.ccr().forgetFollowerAsync(
                request,
                RequestOptions.DEFAULT,
                listener); // <1>
        // end::ccr-forget-follower-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutAutoFollowPattern() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::ccr-put-auto-follow-pattern-request
        PutAutoFollowPatternRequest request =
            new PutAutoFollowPatternRequest(
                "my_pattern", // <1>
                "local", // <2>
                Arrays.asList("logs-*", "metrics-*") // <3>
        );
        request.setFollowIndexNamePattern("copy-{{leader_index}}"); // <4>
        // end::ccr-put-auto-follow-pattern-request

        // tag::ccr-put-auto-follow-pattern-execute
        AcknowledgedResponse response = client.ccr()
            .putAutoFollowPattern(request, RequestOptions.DEFAULT);
        // end::ccr-put-auto-follow-pattern-execute

        // tag::ccr-put-auto-follow-pattern-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ccr-put-auto-follow-pattern-response

        // Delete auto follow pattern, so that we can store it again:
        {
            final DeleteAutoFollowPatternRequest deleteRequest = new DeleteAutoFollowPatternRequest("my_pattern");
            AcknowledgedResponse deleteResponse = client.ccr().deleteAutoFollowPattern(deleteRequest, RequestOptions.DEFAULT);
            assertThat(deleteResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-put-auto-follow-pattern-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) { // <1>
                    boolean acknowledged = response.isAcknowledged();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-put-auto-follow-pattern-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-put-auto-follow-pattern-execute-async
        client.ccr().putAutoFollowPatternAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-put-auto-follow-pattern-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        // Cleanup:
        {
            final DeleteAutoFollowPatternRequest deleteRequest = new DeleteAutoFollowPatternRequest("my_pattern");
            AcknowledgedResponse deleteResponse = client.ccr().deleteAutoFollowPattern(deleteRequest, RequestOptions.DEFAULT);
            assertThat(deleteResponse.isAcknowledged(), is(true));
        }
    }

    public void testDeleteAutoFollowPattern() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // Put auto follow pattern, so that we can delete it:
        {
            final PutAutoFollowPatternRequest putRequest =
                new PutAutoFollowPatternRequest("my_pattern", "local", Collections.singletonList("logs-*"));
            AcknowledgedResponse putResponse = client.ccr().putAutoFollowPattern(putRequest, RequestOptions.DEFAULT);
            assertThat(putResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-delete-auto-follow-pattern-request
        DeleteAutoFollowPatternRequest request =
            new DeleteAutoFollowPatternRequest("my_pattern"); // <1>
        // end::ccr-delete-auto-follow-pattern-request

        // tag::ccr-delete-auto-follow-pattern-execute
        AcknowledgedResponse response = client.ccr()
            .deleteAutoFollowPattern(request, RequestOptions.DEFAULT);
        // end::ccr-delete-auto-follow-pattern-execute

        // tag::ccr-delete-auto-follow-pattern-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ccr-delete-auto-follow-pattern-response

        // Put auto follow pattern, so that we can delete it again:
        {
            final PutAutoFollowPatternRequest putRequest =
                new PutAutoFollowPatternRequest("my_pattern", "local", Collections.singletonList("logs-*"));
            AcknowledgedResponse putResponse = client.ccr().putAutoFollowPattern(putRequest, RequestOptions.DEFAULT);
            assertThat(putResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-delete-auto-follow-pattern-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) { // <1>
                    boolean acknowledged = response.isAcknowledged();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-delete-auto-follow-pattern-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-delete-auto-follow-pattern-execute-async
        client.ccr().deleteAutoFollowPatternAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-delete-auto-follow-pattern-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetAutoFollowPattern() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // Put auto follow pattern, so that we can get it:
        {
            final PutAutoFollowPatternRequest putRequest =
                new PutAutoFollowPatternRequest("my_pattern", "local", Collections.singletonList("logs-*"));
            AcknowledgedResponse putResponse = client.ccr().putAutoFollowPattern(putRequest, RequestOptions.DEFAULT);
            assertThat(putResponse.isAcknowledged(), is(true));
        }

        // tag::ccr-get-auto-follow-pattern-request
        GetAutoFollowPatternRequest request =
            new GetAutoFollowPatternRequest("my_pattern"); // <1>
        // end::ccr-get-auto-follow-pattern-request

        // tag::ccr-get-auto-follow-pattern-execute
        GetAutoFollowPatternResponse response = client.ccr()
            .getAutoFollowPattern(request, RequestOptions.DEFAULT);
        // end::ccr-get-auto-follow-pattern-execute

        // tag::ccr-get-auto-follow-pattern-response
        Map<String, Pattern> patterns = response.getPatterns();
        Pattern pattern = patterns.get("my_pattern"); // <1>
        pattern.getLeaderIndexPatterns();
        // end::ccr-get-auto-follow-pattern-response

        // tag::ccr-get-auto-follow-pattern-execute-listener
        ActionListener<GetAutoFollowPatternResponse> listener =
            new ActionListener<GetAutoFollowPatternResponse>() {
                @Override
                public void onResponse(GetAutoFollowPatternResponse
                                           response) { // <1>
                    Map<String, Pattern> patterns = response.getPatterns();
                    Pattern pattern = patterns.get("my_pattern");
                    pattern.getLeaderIndexPatterns();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-get-auto-follow-pattern-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-get-auto-follow-pattern-execute-async
        client.ccr().getAutoFollowPatternAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-get-auto-follow-pattern-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        // Cleanup:
        {
            DeleteAutoFollowPatternRequest deleteRequest = new DeleteAutoFollowPatternRequest("my_pattern");
            AcknowledgedResponse deleteResponse = client.ccr().deleteAutoFollowPattern(deleteRequest, RequestOptions.DEFAULT);
            assertThat(deleteResponse.isAcknowledged(), is(true));
        }
    }

    public void testGetCCRStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::ccr-get-stats-request
        CcrStatsRequest request =
            new CcrStatsRequest(); // <1>
        // end::ccr-get-stats-request

        // tag::ccr-get-stats-execute
        CcrStatsResponse response = client.ccr()
            .getCcrStats(request, RequestOptions.DEFAULT);
        // end::ccr-get-stats-execute

        // tag::ccr-get-stats-response
        IndicesFollowStats indicesFollowStats =
            response.getIndicesFollowStats(); // <1>
        AutoFollowStats autoFollowStats =
            response.getAutoFollowStats(); // <2>
        // end::ccr-get-stats-response

        // tag::ccr-get-stats-execute-listener
        ActionListener<CcrStatsResponse> listener =
            new ActionListener<CcrStatsResponse>() {
                @Override
                public void onResponse(CcrStatsResponse response) { // <1>
                    IndicesFollowStats indicesFollowStats =
                        response.getIndicesFollowStats();
                    AutoFollowStats autoFollowStats =
                        response.getAutoFollowStats();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-get-stats-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-get-stats-execute-async
        client.ccr().getCcrStatsAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-get-stats-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetFollowStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // Create leader index:
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
            createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
        {
            // Follow index, so that we can query for follow stats:
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", "follower", ActiveShardCount.ONE);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));
        }

        // tag::ccr-get-follow-stats-request
        FollowStatsRequest request =
            new FollowStatsRequest("follower"); // <1>
        // end::ccr-get-follow-stats-request

        // tag::ccr-get-follow-stats-execute
        FollowStatsResponse response = client.ccr()
            .getFollowStats(request, RequestOptions.DEFAULT);
        // end::ccr-get-follow-stats-execute

        // tag::ccr-get-follow-stats-response
        IndicesFollowStats indicesFollowStats =
            response.getIndicesFollowStats(); // <1>
        // end::ccr-get-follow-stats-response

        // tag::ccr-get-follow-stats-execute-listener
        ActionListener<FollowStatsResponse> listener =
            new ActionListener<FollowStatsResponse>() {
                @Override
                public void onResponse(FollowStatsResponse response) { // <1>
                    IndicesFollowStats indicesFollowStats =
                        response.getIndicesFollowStats();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-get-follow-stats-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-get-follow-stats-execute-async
        client.ccr().getFollowStatsAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-get-follow-stats-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
            AcknowledgedResponse pauseFollowResponse =  client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseFollowResponse.isAcknowledged(), is(true));
        }
    }

    public void testGetFollowInfos() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // Create leader index:
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
            createIndexRequest.settings(Collections.singletonMap("index.soft_deletes.enabled", true));
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }
        {
            // Follow index, so that we can query for follow stats:
            PutFollowRequest putFollowRequest = new PutFollowRequest("local", "leader", "follower", ActiveShardCount.ONE);
            PutFollowResponse putFollowResponse = client.ccr().putFollow(putFollowRequest, RequestOptions.DEFAULT);
            assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
            assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
            assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));
        }

        // tag::ccr-get-follow-info-request
        FollowInfoRequest request =
            new FollowInfoRequest("follower"); // <1>
        // end::ccr-get-follow-info-request

        // tag::ccr-get-follow-info-execute
        FollowInfoResponse response = client.ccr()
            .getFollowInfo(request, RequestOptions.DEFAULT);
        // end::ccr-get-follow-info-execute

        // tag::ccr-get-follow-info-response
        List<FollowInfoResponse.FollowerInfo> infos =
            response.getInfos(); // <1>
        // end::ccr-get-follow-info-response

        // tag::ccr-get-follow-info-execute-listener
        ActionListener<FollowInfoResponse> listener =
            new ActionListener<FollowInfoResponse>() {
                @Override
                public void onResponse(FollowInfoResponse response) { // <1>
                    List<FollowInfoResponse.FollowerInfo> infos =
                        response.getInfos();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ccr-get-follow-info-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ccr-get-follow-info-execute-async
        client.ccr().getFollowInfoAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ccr-get-follow-info-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        {
            PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
            AcknowledgedResponse pauseFollowResponse =  client.ccr().pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
            assertThat(pauseFollowResponse.isAcknowledged(), is(true));
        }
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
