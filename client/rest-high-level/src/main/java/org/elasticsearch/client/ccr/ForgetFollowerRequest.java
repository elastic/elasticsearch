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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a forget follower request. Note that this an expert API intended to be used only when unfollowing a follower index fails to
 * remove the follower retention leases. Please be sure that you understand the purpose this API before using.
 */
public final class ForgetFollowerRequest implements ToXContentObject, Validatable {

    private final String followerCluster;

    private final String followerIndex;

    private final String followerIndexUUID;

    private final String leaderRemoteCluster;

    private final String leaderIndex;

    /**
     * The name of the leader index.
     *
     * @return the name of the leader index
     */
    public String leaderIndex() {
        return leaderIndex;
    }

    /**
     * Construct a forget follower request.
     *
     * @param followerCluster     the name of the cluster containing the follower index to forget
     * @param followerIndex       the name of follower index
     * @param followerIndexUUID   the UUID of the follower index
     * @param leaderRemoteCluster the alias of the remote cluster containing the leader index from the perspective of the follower index
     * @param leaderIndex         the name of the leader index
     */
    public ForgetFollowerRequest(
            final String followerCluster,
            final String followerIndex,
            final String followerIndexUUID,
            final String leaderRemoteCluster,
            final String leaderIndex) {
        this.followerCluster = Objects.requireNonNull(followerCluster);
        this.followerIndex = Objects.requireNonNull(followerIndex);
        this.followerIndexUUID = Objects.requireNonNull(followerIndexUUID);
        this.leaderRemoteCluster = Objects.requireNonNull(leaderRemoteCluster);
        this.leaderIndex = Objects.requireNonNull(leaderIndex);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field("follower_cluster", followerCluster);
            builder.field("follower_index", followerIndex);
            builder.field("follower_index_uuid", followerIndexUUID);
            builder.field("leader_remote_cluster", leaderRemoteCluster);
        }
        builder.endObject();
        return builder;
    }

}
