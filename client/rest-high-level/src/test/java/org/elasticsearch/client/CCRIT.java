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

package org.elasticsearch.client;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.client.ccr.IndicesFollowStats;
import org.elasticsearch.client.ccr.IndicesFollowStats.ShardFollowStats;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CCRIT extends ESRestHighLevelClientTestCase {

    @Before
    public void setupRemoteClusterConfig() throws Exception {
        setupRemoteClusterConfig("local_cluster");
    }

    public void testIndexFollowing() throws Exception {
        CcrClient ccrClient = highLevelClient().ccr();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
        CreateIndexResponse response = highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), is(true));

        PutFollowRequest putFollowRequest = new PutFollowRequest("local_cluster", "leader", "follower", ActiveShardCount.ONE);
        PutFollowResponse putFollowResponse = execute(putFollowRequest, ccrClient::putFollow, ccrClient::putFollowAsync);
        assertThat(putFollowResponse.isFollowIndexCreated(), is(true));
        assertThat(putFollowResponse.isFollowIndexShardsAcked(), is(true));
        assertThat(putFollowResponse.isIndexFollowingStarted(), is(true));

        IndexRequest indexRequest = new IndexRequest("leader")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source("{}", XContentType.JSON);
        highLevelClient().index(indexRequest, RequestOptions.DEFAULT);

        SearchRequest leaderSearchRequest = new SearchRequest("leader");
        SearchResponse leaderSearchResponse = highLevelClient().search(leaderSearchRequest, RequestOptions.DEFAULT);
        assertThat(leaderSearchResponse.getHits().getTotalHits().value, equalTo(1L));

        try {
            assertBusy(() -> {
                FollowInfoRequest followInfoRequest = new FollowInfoRequest("follower");
                FollowInfoResponse followInfoResponse =
                    execute(followInfoRequest, ccrClient::getFollowInfo, ccrClient::getFollowInfoAsync);
                assertThat(followInfoResponse.getInfos().size(), equalTo(1));
                assertThat(followInfoResponse.getInfos().get(0).getFollowerIndex(), equalTo("follower"));
                assertThat(followInfoResponse.getInfos().get(0).getLeaderIndex(), equalTo("leader"));
                assertThat(followInfoResponse.getInfos().get(0).getRemoteCluster(), equalTo("local_cluster"));
                assertThat(followInfoResponse.getInfos().get(0).getStatus(), equalTo(FollowInfoResponse.Status.ACTIVE));

                FollowStatsRequest followStatsRequest = new FollowStatsRequest("follower");
                FollowStatsResponse followStatsResponse =
                    execute(followStatsRequest, ccrClient::getFollowStats, ccrClient::getFollowStatsAsync);
                List<ShardFollowStats> shardFollowStats = followStatsResponse.getIndicesFollowStats().getShardFollowStats("follower");
                long followerGlobalCheckpoint = shardFollowStats.stream()
                    .mapToLong(ShardFollowStats::getFollowerGlobalCheckpoint)
                    .max()
                    .getAsLong();
                assertThat(followerGlobalCheckpoint, equalTo(0L));

                SearchRequest followerSearchRequest = new SearchRequest("follower");
                SearchResponse followerSearchResponse = highLevelClient().search(followerSearchRequest, RequestOptions.DEFAULT);
                assertThat(followerSearchResponse.getHits().getTotalHits().value, equalTo(1L));
            });
        } catch (Exception e) {
            IndicesFollowStats followStats = ccrClient.getCcrStats(new CcrStatsRequest(), RequestOptions.DEFAULT).getIndicesFollowStats();
            for (Map.Entry<String, List<ShardFollowStats>> entry : followStats.getShardFollowStats().entrySet()) {
                for (ShardFollowStats shardFollowStats : entry.getValue()) {
                    if (shardFollowStats.getFatalException() != null) {
                        logger.warn(new ParameterizedMessage("fatal shard follow exception {}", shardFollowStats.getShardId()),
                            shardFollowStats.getFatalException());
                    }
                }
            }
        }

        PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
        AcknowledgedResponse pauseFollowResponse = execute(pauseFollowRequest, ccrClient::pauseFollow, ccrClient::pauseFollowAsync);
        assertThat(pauseFollowResponse.isAcknowledged(), is(true));

        highLevelClient().index(indexRequest, RequestOptions.DEFAULT);

        ResumeFollowRequest resumeFollowRequest = new ResumeFollowRequest("follower");
        AcknowledgedResponse resumeFollowResponse = execute(resumeFollowRequest, ccrClient::resumeFollow, ccrClient::resumeFollowAsync);
        assertThat(resumeFollowResponse.isAcknowledged(), is(true));

        assertBusy(() -> {
            FollowStatsRequest followStatsRequest = new FollowStatsRequest("follower");
            FollowStatsResponse followStatsResponse =
                execute(followStatsRequest, ccrClient::getFollowStats, ccrClient::getFollowStatsAsync);
            List<ShardFollowStats> shardFollowStats = followStatsResponse.getIndicesFollowStats().getShardFollowStats("follower");
            long followerGlobalCheckpoint = shardFollowStats.stream()
                .mapToLong(ShardFollowStats::getFollowerGlobalCheckpoint)
                .max()
                .getAsLong();
            assertThat(followerGlobalCheckpoint, equalTo(1L));

            SearchRequest followerSearchRequest = new SearchRequest("follower");
            SearchResponse followerSearchResponse = highLevelClient().search(followerSearchRequest, RequestOptions.DEFAULT);
            assertThat(followerSearchResponse.getHits().getTotalHits().value, equalTo(2L));
        });

        // Need to pause prior to unfollowing it:
        pauseFollowRequest = new PauseFollowRequest("follower");
        pauseFollowResponse = execute(pauseFollowRequest, ccrClient::pauseFollow, ccrClient::pauseFollowAsync);
        assertThat(pauseFollowResponse.isAcknowledged(), is(true));

        assertBusy(() -> {
            FollowInfoRequest followInfoRequest = new FollowInfoRequest("follower");
            FollowInfoResponse followInfoResponse =
                execute(followInfoRequest, ccrClient::getFollowInfo, ccrClient::getFollowInfoAsync);
            assertThat(followInfoResponse.getInfos().size(), equalTo(1));
            assertThat(followInfoResponse.getInfos().get(0).getFollowerIndex(), equalTo("follower"));
            assertThat(followInfoResponse.getInfos().get(0).getLeaderIndex(), equalTo("leader"));
            assertThat(followInfoResponse.getInfos().get(0).getRemoteCluster(), equalTo("local_cluster"));
            assertThat(followInfoResponse.getInfos().get(0).getStatus(), equalTo(FollowInfoResponse.Status.PAUSED));
        });

        // Need to close index prior to unfollowing it:
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest("follower");
        org.elasticsearch.action.support.master.AcknowledgedResponse closeIndexReponse =
            highLevelClient().indices().close(closeIndexRequest, RequestOptions.DEFAULT);
        assertThat(closeIndexReponse.isAcknowledged(), is(true));

        UnfollowRequest unfollowRequest = new UnfollowRequest("follower");
        AcknowledgedResponse unfollowResponse = execute(unfollowRequest, ccrClient::unfollow, ccrClient::unfollowAsync);
        assertThat(unfollowResponse.isAcknowledged(), is(true));
    }

    public void testForgetFollower() throws IOException {
        final CcrClient ccrClient = highLevelClient().ccr();

        final CreateIndexRequest createIndexRequest = new CreateIndexRequest("leader");
        final Map<String, String> settings = new HashMap<>(3);
        final int numberOfShards = randomIntBetween(1, 2);
        settings.put("index.number_of_replicas", "0");
        settings.put("index.number_of_shards", Integer.toString(numberOfShards));
        createIndexRequest.settings(settings);
        final CreateIndexResponse response = highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), is(true));

        final PutFollowRequest putFollowRequest = new PutFollowRequest("local_cluster", "leader", "follower", ActiveShardCount.ONE);
        final PutFollowResponse putFollowResponse = execute(putFollowRequest, ccrClient::putFollow, ccrClient::putFollowAsync);
        assertTrue(putFollowResponse.isFollowIndexCreated());
        assertTrue(putFollowResponse.isFollowIndexShardsAcked());
        assertTrue(putFollowResponse.isIndexFollowingStarted());

        final String clusterName = highLevelClient().info(RequestOptions.DEFAULT).getClusterName();

        final Request statsRequest = new Request("GET", "/follower/_stats");
        final Response statsResponse = client().performRequest(statsRequest);
        final ObjectPath statsObjectPath = ObjectPath.createFromResponse(statsResponse);
        final String followerIndexUUID = statsObjectPath.evaluate("indices.follower.uuid");

        final PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("follower");
        AcknowledgedResponse pauseFollowResponse = execute(pauseFollowRequest, ccrClient::pauseFollow, ccrClient::pauseFollowAsync);
        assertTrue(pauseFollowResponse.isAcknowledged());

        final ForgetFollowerRequest forgetFollowerRequest =
                new ForgetFollowerRequest(clusterName, "follower", followerIndexUUID, "local_cluster", "leader");
        final BroadcastResponse forgetFollowerResponse =
                execute(forgetFollowerRequest, ccrClient::forgetFollower, ccrClient::forgetFollowerAsync);
        assertThat(forgetFollowerResponse.shards().total(), equalTo(numberOfShards));
        assertThat(forgetFollowerResponse.shards().successful(), equalTo(numberOfShards));
        assertThat(forgetFollowerResponse.shards().skipped(), equalTo(0));
        assertThat(forgetFollowerResponse.shards().failed(), equalTo(0));
        assertThat(forgetFollowerResponse.shards().failures(), empty());

        final Request retentionLeasesRequest = new Request("GET", "/leader/_stats");
        retentionLeasesRequest.addParameter("level", "shards");
        final Response retentionLeasesResponse = client().performRequest(retentionLeasesRequest);
        final Map<?, ?> shardsStats = ObjectPath.createFromResponse(retentionLeasesResponse).evaluate("indices.leader.shards");
        assertThat(shardsStats.keySet(), hasSize(numberOfShards));
        for (int i = 0; i < numberOfShards; i++) {
            final List<?> shardStats = (List<?>) shardsStats.get(Integer.toString(i));
            assertThat(shardStats, hasSize(1));
            final Map<?, ?> shardStatsAsMap = (Map<?, ?>) shardStats.get(0);
            final Map<?, ?> retentionLeasesStats = (Map<?, ?>) shardStatsAsMap.get("retention_leases");
            final List<?> leases = (List<?>) retentionLeasesStats.get("leases");
            for (final Object lease : leases) {
                assertThat(((Map<?, ?>) lease).get("source"), equalTo(ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE));
            }
        }
    }

    public void testAutoFollowing() throws Exception {
        CcrClient ccrClient = highLevelClient().ccr();
        PutAutoFollowPatternRequest putAutoFollowPatternRequest =
            new PutAutoFollowPatternRequest("pattern1", "local_cluster", Collections.singletonList("logs-*"));
        putAutoFollowPatternRequest.setFollowIndexNamePattern("copy-{{leader_index}}");
        AcknowledgedResponse putAutoFollowPatternResponse =
            execute(putAutoFollowPatternRequest, ccrClient::putAutoFollowPattern, ccrClient::putAutoFollowPatternAsync);
        assertThat(putAutoFollowPatternResponse.isAcknowledged(), is(true));

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("logs-20200101");
        CreateIndexResponse response = highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertThat(response.isAcknowledged(), is(true));

        assertBusy(() -> {
            CcrStatsRequest ccrStatsRequest = new CcrStatsRequest();
            CcrStatsResponse ccrStatsResponse = execute(ccrStatsRequest, ccrClient::getCcrStats, ccrClient::getCcrStatsAsync);
            assertThat(ccrStatsResponse.getAutoFollowStats().getNumberOfSuccessfulFollowIndices(), equalTo(1L));
            assertThat(ccrStatsResponse.getIndicesFollowStats().getShardFollowStats("copy-logs-20200101"), notNullValue());
        });
        assertThat(indexExists("copy-logs-20200101"), is(true));

        GetAutoFollowPatternRequest getAutoFollowPatternRequest =
            randomBoolean() ? new GetAutoFollowPatternRequest("pattern1") : new GetAutoFollowPatternRequest();
        GetAutoFollowPatternResponse getAutoFollowPatternResponse =
            execute(getAutoFollowPatternRequest, ccrClient::getAutoFollowPattern, ccrClient::getAutoFollowPatternAsync);
        assertThat(getAutoFollowPatternResponse.getPatterns().size(), equalTo(1));
        GetAutoFollowPatternResponse.Pattern pattern = getAutoFollowPatternResponse.getPatterns().get("pattern1");
        assertThat(pattern, notNullValue());
        assertThat(pattern.getRemoteCluster(), equalTo(putAutoFollowPatternRequest.getRemoteCluster()));
        assertThat(pattern.getLeaderIndexPatterns(), equalTo(putAutoFollowPatternRequest.getLeaderIndexPatterns()));
        assertThat(pattern.getFollowIndexNamePattern(), equalTo(putAutoFollowPatternRequest.getFollowIndexNamePattern()));

        // Cleanup:
        final DeleteAutoFollowPatternRequest deleteAutoFollowPatternRequest = new DeleteAutoFollowPatternRequest("pattern1");
        AcknowledgedResponse deleteAutoFollowPatternResponse =
            execute(deleteAutoFollowPatternRequest, ccrClient::deleteAutoFollowPattern, ccrClient::deleteAutoFollowPatternAsync);
        assertThat(deleteAutoFollowPatternResponse.isAcknowledged(), is(true));

        PauseFollowRequest pauseFollowRequest = new PauseFollowRequest("copy-logs-20200101");
        AcknowledgedResponse pauseFollowResponse = ccrClient.pauseFollow(pauseFollowRequest, RequestOptions.DEFAULT);
        assertThat(pauseFollowResponse.isAcknowledged(), is(true));
    }

}
