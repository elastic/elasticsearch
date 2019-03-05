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

public final class ForgetFollowerRequest implements ToXContentObject, Validatable {

    private final String followerCluster;

    private final String followerIndex;

    private final String followerIndexUUID;

    private final String leaderRemoteCluster;

    private final String leaderIndex;

    public String leaderIndex() {
        return leaderIndex;
    }

    public ForgetFollowerRequest(
            String followerCluster,
            String followerIndex,
            String followerIndexUUID,
            String leaderRemoteCluster,
            String leaderIndex) {
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
