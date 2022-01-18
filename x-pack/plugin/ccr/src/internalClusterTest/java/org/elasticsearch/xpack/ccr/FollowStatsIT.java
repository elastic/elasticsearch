/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.LocalIndexFollowingIT.getIndexSettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;

/*
 * Test scope is important to ensure that other tests added to this suite do not interfere with the expectation in
 * testStatsWhenNoPersistentTasksMetadataExists that the cluster state does not contain any persistent tasks metadata.
 */
public class FollowStatsIT extends CcrSingleNodeTestCase {

    /**
     * Previously we would throw a NullPointerException when there was no persistent tasks metadata in the cluster state. This tests
     * maintains that we do not make this mistake again.
     *
     * @throws InterruptedException if we are interrupted waiting on the latch to countdown
     */
    public void testStatsWhenNoPersistentTasksMetadataExists() throws InterruptedException {
        final ClusterStateResponse response = client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        assertNull(response.getState().metadata().custom(PersistentTasksCustomMetadata.TYPE));
        final AtomicBoolean onResponse = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            FollowStatsAction.INSTANCE,
            new FollowStatsAction.StatsRequest(),
            new ActionListener<FollowStatsAction.StatsResponses>() {
                @Override
                public void onResponse(final FollowStatsAction.StatsResponses statsResponses) {
                    try {
                        assertThat(statsResponses.getTaskFailures(), empty());
                        assertThat(statsResponses.getNodeFailures(), empty());
                        onResponse.set(true);
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    try {
                        fail(e.toString());
                    } finally {
                        latch.countDown();
                    }
                }
            }
        );
        latch.await();
        assertTrue(onResponse.get());
    }

    public void testFollowStatsApiFollowerIndexFiltering() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");
        assertAcked(client().admin().indices().prepareCreate("leader2").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader2");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        followRequest = getPutFollowRequest("leader2", "follower2");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setIndices(new String[] { "follower1" });
        FollowStatsAction.StatsResponses response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setIndices(new String[] { "follower2" });
        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower2"));

        response = client().execute(FollowStatsAction.INSTANCE, new FollowStatsAction.StatsRequest()).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(2));
        response.getStatsResponses().sort(Comparator.comparing(o -> o.status().followerIndex()));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));
        assertThat(response.getStatsResponses().get(1).status().followerIndex(), equalTo("follower2"));

        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower1")).actionGet());
        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower2")).actionGet());

        assertBusy(() -> {
            List<FollowStatsAction.StatsResponse> responseList = client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.Request())
                .actionGet()
                .getFollowStats()
                .getStatsResponses();
            assertThat(responseList.size(), equalTo(0));
        });
    }

    public void testFollowStatsApiResourceNotFound() throws Exception {
        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        FollowStatsAction.StatsResponses response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(0));

        statsRequest.setIndices(new String[] { "follower1" });
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet()
        );
        assertThat(e.getMessage(), equalTo("No shard follow tasks for follower indices [follower1]"));

        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        statsRequest.setIndices(new String[] { "follower2" });
        e = expectThrows(ResourceNotFoundException.class, () -> client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet());
        assertThat(e.getMessage(), equalTo("No shard follow tasks for follower indices [follower2]"));

        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower1")).actionGet());
    }

    public void testFollowStatsApiWithDeletedFollowerIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        FollowStatsAction.StatsResponses response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setIndices(new String[] { "follower1" });
        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest("follower1")).actionGet());

        assertBusy(() -> {
            FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
            FollowStatsAction.StatsResponses statsResponse = client().execute(FollowStatsAction.INSTANCE, request).actionGet();
            assertThat(statsResponse.getStatsResponses(), hasSize(0));
        });
    }

    public void testFollowStatsApiIncludeShardFollowStatsWithClosedFollowerIndex() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        FollowStatsAction.StatsRequest statsRequest = new FollowStatsAction.StatsRequest();
        FollowStatsAction.StatsResponses response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setIndices(new String[] { "follower1" });
        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        assertAcked(client().admin().indices().close(new CloseIndexRequest("follower1")).actionGet());

        statsRequest = new FollowStatsAction.StatsRequest();
        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        statsRequest = new FollowStatsAction.StatsRequest();
        statsRequest.setIndices(new String[] { "follower1" });
        response = client().execute(FollowStatsAction.INSTANCE, statsRequest).actionGet();
        assertThat(response.getStatsResponses().size(), equalTo(1));
        assertThat(response.getStatsResponses().get(0).status().followerIndex(), equalTo("follower1"));

        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower1")).actionGet());
    }

}
