/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.Collections;
import java.util.Comparator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.LocalIndexFollowingIT.getIndexSettings;
import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FollowInfoIT extends CcrSingleNodeTestCase {

    public void testFollowInfoApiFollowerIndexFiltering() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");
        assertAcked(client().admin().indices().prepareCreate("leader2").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader2");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        followRequest = getPutFollowRequest("leader2", "follower2");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        FollowInfoAction.Request request = new FollowInfoAction.Request();
        request.setFollowerIndices("follower1");
        FollowInfoAction.Response response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        assertThat(response.getFollowInfos().size(), equalTo(1));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower1"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader1"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(0).getParameters(), notNullValue());

        request = new FollowInfoAction.Request();
        request.setFollowerIndices("follower2");
        response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        assertThat(response.getFollowInfos().size(), equalTo(1));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower2"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader2"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(0).getParameters(), notNullValue());

        request = new FollowInfoAction.Request();
        request.setFollowerIndices("_all");
        response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        response.getFollowInfos().sort(Comparator.comparing(FollowInfoAction.Response.FollowerInfo::getFollowerIndex));
        assertThat(response.getFollowInfos().size(), equalTo(2));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower1"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader1"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(0).getParameters(), notNullValue());
        assertThat(response.getFollowInfos().get(1).getFollowerIndex(), equalTo("follower2"));
        assertThat(response.getFollowInfos().get(1).getLeaderIndex(), equalTo("leader2"));
        assertThat(response.getFollowInfos().get(1).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(1).getParameters(), notNullValue());

        // Pause follower1 index and check the follower info api:
        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower1")).actionGet());

        request = new FollowInfoAction.Request();
        request.setFollowerIndices("follower1");
        response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        assertThat(response.getFollowInfos().size(), equalTo(1));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower1"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader1"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.PAUSED));
        assertThat(response.getFollowInfos().get(0).getParameters(), nullValue());

        request = new FollowInfoAction.Request();
        request.setFollowerIndices("follower2");
        response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        assertThat(response.getFollowInfos().size(), equalTo(1));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower2"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader2"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(0).getParameters(), notNullValue());

        request = new FollowInfoAction.Request();
        request.setFollowerIndices("_all");
        response = client().execute(FollowInfoAction.INSTANCE, request).actionGet();
        response.getFollowInfos().sort(Comparator.comparing(FollowInfoAction.Response.FollowerInfo::getFollowerIndex));
        assertThat(response.getFollowInfos().size(), equalTo(2));
        assertThat(response.getFollowInfos().get(0).getFollowerIndex(), equalTo("follower1"));
        assertThat(response.getFollowInfos().get(0).getLeaderIndex(), equalTo("leader1"));
        assertThat(response.getFollowInfos().get(0).getStatus(), equalTo(Status.PAUSED));
        assertThat(response.getFollowInfos().get(0).getParameters(), nullValue());
        assertThat(response.getFollowInfos().get(1).getFollowerIndex(), equalTo("follower2"));
        assertThat(response.getFollowInfos().get(1).getLeaderIndex(), equalTo("leader2"));
        assertThat(response.getFollowInfos().get(1).getStatus(), equalTo(Status.ACTIVE));
        assertThat(response.getFollowInfos().get(1).getParameters(), notNullValue());

        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower2")).actionGet());
    }

    public void testFollowInfoApiIndexMissing() throws Exception {
        final String leaderIndexSettings = getIndexSettings(1, 0, Collections.emptyMap());
        assertAcked(client().admin().indices().prepareCreate("leader1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader1");
        assertAcked(client().admin().indices().prepareCreate("leader2").setSource(leaderIndexSettings, XContentType.JSON));
        ensureGreen("leader2");

        PutFollowAction.Request followRequest = getPutFollowRequest("leader1", "follower1");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        followRequest = getPutFollowRequest("leader2", "follower2");
        client().execute(PutFollowAction.INSTANCE, followRequest).get();

        FollowInfoAction.Request request1 = new FollowInfoAction.Request();
        request1.setFollowerIndices("follower3");
        expectThrows(IndexNotFoundException.class, () -> client().execute(FollowInfoAction.INSTANCE, request1).actionGet());

        FollowInfoAction.Request request2 = new FollowInfoAction.Request();
        request2.setFollowerIndices("follower2", "follower3");
        expectThrows(IndexNotFoundException.class, () -> client().execute(FollowInfoAction.INSTANCE, request2).actionGet());

        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower1")).actionGet());
        assertAcked(client().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request("follower2")).actionGet());
    }

}
