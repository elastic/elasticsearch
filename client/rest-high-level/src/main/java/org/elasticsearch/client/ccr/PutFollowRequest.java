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

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class PutFollowRequest extends FollowConfig implements Validatable, ToXContentObject {

    static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
    static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
    static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");

    private final String remoteCluster;
    private final String leaderIndex;
    private final String followerIndex;

    public PutFollowRequest(String remoteCluster, String leaderIndex, String followerIndex) {
        this.remoteCluster = Objects.requireNonNull(remoteCluster, "remoteCluster");
        this.leaderIndex = Objects.requireNonNull(leaderIndex, "leaderIndex");
        this.followerIndex = Objects.requireNonNull(followerIndex, "followerIndex");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
        builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
        builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PutFollowRequest that = (PutFollowRequest) o;
        return Objects.equals(remoteCluster, that.remoteCluster) &&
            Objects.equals(leaderIndex, that.leaderIndex) &&
            Objects.equals(followerIndex, that.followerIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            remoteCluster,
            leaderIndex,
            followerIndex
        );
    }
}
