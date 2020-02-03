/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

final class WaitForFollowShardTasksStep extends AsyncWaitStep {

    static final String NAME = "wait-for-follow-shard-tasks";

    WaitForFollowShardTasksStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener, TimeValue masterTimeout) {
        Map<String, String> customIndexMetadata = indexMetaData.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(true, null);
            return;
        }

        FollowStatsAction.StatsRequest request = new FollowStatsAction.StatsRequest();
        request.setIndices(new String[]{indexMetaData.getIndex().getName()});
        getClient().execute(FollowStatsAction.INSTANCE, request,
            ActionListener.wrap(r -> handleResponse(r, listener), listener::onFailure));
    }

    void handleResponse(FollowStatsAction.StatsResponses responses, Listener listener) {
        List<ShardFollowNodeTaskStatus> unSyncedShardFollowStatuses = responses.getStatsResponses()
            .stream()
            .map(FollowStatsAction.StatsResponse::status)
            .filter(shardFollowStatus -> shardFollowStatus.leaderGlobalCheckpoint() != shardFollowStatus.followerGlobalCheckpoint())
            .collect(Collectors.toList());

        // Follow stats api needs to return stats for follower index and all shard follow tasks should be synced:
        boolean conditionMet = responses.getStatsResponses().size() > 0 && unSyncedShardFollowStatuses.isEmpty();
        if (conditionMet) {
            listener.onResponse(true, null);
        } else {
            List<Info.ShardFollowTaskInfo> shardFollowTaskInfos = unSyncedShardFollowStatuses
                .stream()
                .map(status -> new Info.ShardFollowTaskInfo(status.followerIndex(), status.getShardId(),
                    status.leaderGlobalCheckpoint(), status.followerGlobalCheckpoint()))
                .collect(Collectors.toList());
            listener.onResponse(false, new Info(shardFollowTaskInfos));
        }
    }

    static final class Info implements ToXContentObject {

        static final ParseField SHARD_FOLLOW_TASKS = new ParseField("shard_follow_tasks");
        static final ParseField MESSAGE = new ParseField("message");

        private final List<ShardFollowTaskInfo> shardFollowTaskInfos;

        Info(List<ShardFollowTaskInfo> shardFollowTaskInfos) {
            this.shardFollowTaskInfos = shardFollowTaskInfos;
        }

        List<ShardFollowTaskInfo> getShardFollowTaskInfos() {
            return shardFollowTaskInfos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SHARD_FOLLOW_TASKS.getPreferredName(), shardFollowTaskInfos);
            String message;
            if (shardFollowTaskInfos.size() > 0) {
                message = "Waiting for [" + shardFollowTaskInfos.size() + "] shard follow tasks to be in sync";
            } else {
                message = "Waiting for following to be unpaused and all shard follow tasks to be up to date";
            }
            builder.field(MESSAGE.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return Objects.equals(shardFollowTaskInfos, info.shardFollowTaskInfos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardFollowTaskInfos);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        static final class ShardFollowTaskInfo implements ToXContentObject {

            static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
            static final ParseField SHARD_ID_FIELD = new ParseField("shard_id");
            static final ParseField LEADER_GLOBAL_CHECKPOINT_FIELD = new ParseField("leader_global_checkpoint");
            static final ParseField FOLLOWER_GLOBAL_CHECKPOINT_FIELD = new ParseField("follower_global_checkpoint");

            private final String followerIndex;
            private final int shardId;
            private final long leaderGlobalCheckpoint;
            private final long followerGlobalCheckpoint;

            ShardFollowTaskInfo(String followerIndex, int shardId, long leaderGlobalCheckpoint, long followerGlobalCheckpoint) {
                this.followerIndex = followerIndex;
                this.shardId = shardId;
                this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
                this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            }

            String getFollowerIndex() {
                return followerIndex;
            }


            int getShardId() {
                return shardId;
            }

            long getLeaderGlobalCheckpoint() {
                return leaderGlobalCheckpoint;
            }

            long getFollowerGlobalCheckpoint() {
                return followerGlobalCheckpoint;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
                builder.field(SHARD_ID_FIELD.getPreferredName(), shardId);
                builder.field(LEADER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), leaderGlobalCheckpoint);
                builder.field(FOLLOWER_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), followerGlobalCheckpoint);
                builder.endObject();
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ShardFollowTaskInfo that = (ShardFollowTaskInfo) o;
                return shardId == that.shardId &&
                    leaderGlobalCheckpoint == that.leaderGlobalCheckpoint &&
                    followerGlobalCheckpoint == that.followerGlobalCheckpoint &&
                    Objects.equals(followerIndex, that.followerIndex);
            }

            @Override
            public int hashCode() {
                return Objects.hash(followerIndex, shardId, leaderGlobalCheckpoint, followerGlobalCheckpoint);
            }
        }
    }
}
