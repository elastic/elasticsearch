/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class PutFollowRequest extends FollowConfig implements Validatable, ToXContentObject {

    static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
    static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");

    private final String remoteCluster;
    private final String leaderIndex;
    private final String followerIndex;
    private final ActiveShardCount waitForActiveShards;

    public PutFollowRequest(String remoteCluster, String leaderIndex, String followerIndex) {
        this(remoteCluster, leaderIndex, followerIndex, ActiveShardCount.NONE);
    }

    public PutFollowRequest(String remoteCluster, String leaderIndex, String followerIndex, ActiveShardCount waitForActiveShards) {
        this.remoteCluster = Objects.requireNonNull(remoteCluster, "remoteCluster");
        this.leaderIndex = Objects.requireNonNull(leaderIndex, "leaderIndex");
        this.followerIndex = Objects.requireNonNull(followerIndex, "followerIndex");
        this.waitForActiveShards = waitForActiveShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
        builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    public String getLeaderIndex() {
        return leaderIndex;
    }

    public String getFollowerIndex() {
        return followerIndex;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        PutFollowRequest that = (PutFollowRequest) o;
        return Objects.equals(waitForActiveShards, that.waitForActiveShards) &&
            Objects.equals(remoteCluster, that.remoteCluster) &&
            Objects.equals(leaderIndex, that.leaderIndex) &&
            Objects.equals(followerIndex, that.followerIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            remoteCluster,
            leaderIndex,
            followerIndex,
            waitForActiveShards);
    }
}
