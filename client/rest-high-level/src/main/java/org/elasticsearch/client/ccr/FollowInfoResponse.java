/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

public final class FollowInfoResponse {

    static final ParseField FOLLOWER_INDICES_FIELD = new ParseField("follower_indices");

    private static final ConstructingObjectParser<FollowInfoResponse, Void> PARSER = new ConstructingObjectParser<>(
        "indices",
        true,
        args -> {
            @SuppressWarnings("unchecked")
            List<FollowerInfo> infos = (List<FollowerInfo>) args[0];
            return new FollowInfoResponse(infos);
        });

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), FollowerInfo.PARSER, FOLLOWER_INDICES_FIELD);
    }

    public static FollowInfoResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<FollowerInfo> infos;

    FollowInfoResponse(List<FollowerInfo> infos) {
        this.infos = infos;
    }

    public List<FollowerInfo> getInfos() {
        return infos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FollowInfoResponse that = (FollowInfoResponse) o;
        return infos.equals(that.infos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(infos);
    }

    public static final class FollowerInfo {

        static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
        static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
        static final ParseField STATUS_FIELD = new ParseField("status");
        static final ParseField PARAMETERS_FIELD = new ParseField("parameters");

        private static final ConstructingObjectParser<FollowerInfo, Void> PARSER = new ConstructingObjectParser<>(
            "follower_info",
            true,
            args -> {
                return new FollowerInfo((String) args[0], (String) args[1], (String) args[2],
                    Status.fromString((String) args[3]), (FollowConfig) args[4]);
            });

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FOLLOWER_INDEX_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REMOTE_CLUSTER_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> FollowConfig.fromXContent(p), PARAMETERS_FIELD);
        }

        private final String followerIndex;
        private final String remoteCluster;
        private final String leaderIndex;
        private final Status status;
        private final FollowConfig parameters;

        FollowerInfo(String followerIndex, String remoteCluster, String leaderIndex, Status status,
                            FollowConfig parameters) {
            this.followerIndex = followerIndex;
            this.remoteCluster = remoteCluster;
            this.leaderIndex = leaderIndex;
            this.status = status;
            this.parameters = parameters;
        }

        public String getFollowerIndex() {
            return followerIndex;
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public Status getStatus() {
            return status;
        }

        public FollowConfig getParameters() {
            return parameters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FollowerInfo that = (FollowerInfo) o;
            return Objects.equals(followerIndex, that.followerIndex) &&
                Objects.equals(remoteCluster, that.remoteCluster) &&
                Objects.equals(leaderIndex, that.leaderIndex) &&
                status == that.status &&
                Objects.equals(parameters, that.parameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(followerIndex, remoteCluster, leaderIndex, status, parameters);
        }

    }

    public enum Status {

        ACTIVE("active"),
        PAUSED("paused");

        private final String name;

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static Status fromString(String value) {
            switch (value) {
                case "active":
                    return Status.ACTIVE;
                case "paused":
                    return Status.PAUSED;
                default:
                    throw new IllegalArgumentException("unexpected status value [" + value + "]");
            }
        }
    }
}
