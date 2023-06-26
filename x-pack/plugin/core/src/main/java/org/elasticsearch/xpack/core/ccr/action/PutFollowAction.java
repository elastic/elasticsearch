/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class PutFollowAction extends ActionType<PutFollowAction.Response> {

    public static final PutFollowAction INSTANCE = new PutFollowAction();
    public static final String NAME = "indices:admin/xpack/ccr/put_follow";

    private PutFollowAction() {
        super(NAME, PutFollowAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        private static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        private static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
        private static final ParseField SETTINGS_FIELD = new ParseField("settings");
        private static final ParseField DATA_STREAM_NAME = new ParseField("data_stream_name");

        // Note that Request should be the Value class here for this parser with a 'parameters' field that maps to
        // PutFollowParameters class. But since two minor version are already released with duplicate follow parameters
        // in several APIs, PutFollowParameters is now the Value class here.
        private static final ObjectParser<PutFollowParameters, Void> PARSER = new ObjectParser<>(NAME, PutFollowParameters::new);

        static {
            PARSER.declareString((putFollowParameters, value) -> putFollowParameters.remoteCluster = value, REMOTE_CLUSTER_FIELD);
            PARSER.declareString((putFollowParameters, value) -> putFollowParameters.leaderIndex = value, LEADER_INDEX_FIELD);
            PARSER.declareString((putFollowParameters, value) -> putFollowParameters.dataStreamName = value, DATA_STREAM_NAME);
            PARSER.declareObject(
                (putFollowParameters, value) -> putFollowParameters.settings = value,
                (p, c) -> Settings.fromXContent(p),
                SETTINGS_FIELD
            );
            FollowParameters.initParser(PARSER);
        }

        public static Request fromXContent(final XContentParser parser, final String followerIndex, ActiveShardCount waitForActiveShards)
            throws IOException {
            PutFollowParameters parameters = PARSER.parse(parser, null);

            Request request = new Request();
            request.waitForActiveShards(waitForActiveShards);
            request.setFollowerIndex(followerIndex);
            request.setRemoteCluster(parameters.remoteCluster);
            request.setLeaderIndex(parameters.leaderIndex);
            request.setDataStreamName(parameters.dataStreamName);
            request.setSettings(parameters.settings);
            request.setParameters(parameters);
            return request;
        }

        private String remoteCluster;
        private String leaderIndex;
        private String followerIndex;
        @Nullable
        private String dataStreamName;
        private Settings settings = Settings.EMPTY;
        private FollowParameters parameters = new FollowParameters();
        private ActiveShardCount waitForActiveShards = ActiveShardCount.NONE;

        public Request() {}

        public String getFollowerIndex() {
            return followerIndex;
        }

        public void setFollowerIndex(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public void setRemoteCluster(String remoteCluster) {
            this.remoteCluster = remoteCluster;
        }

        public String getLeaderIndex() {
            return leaderIndex;
        }

        public void setLeaderIndex(String leaderIndex) {
            this.leaderIndex = leaderIndex;
        }

        public Settings getSettings() {
            return settings;
        }

        public void setSettings(final Settings settings) {
            this.settings = Objects.requireNonNull(settings);
        }

        public FollowParameters getParameters() {
            return parameters;
        }

        public void setParameters(FollowParameters parameters) {
            this.parameters = parameters;
        }

        @Nullable
        public String getDataStreamName() {
            return dataStreamName;
        }

        public void setDataStreamName(String dataStreamName) {
            this.dataStreamName = dataStreamName;
        }

        public ActiveShardCount waitForActiveShards() {
            return waitForActiveShards;
        }

        /**
         * Sets the number of shard copies that should be active for follower index creation to
         * return. Defaults to {@link ActiveShardCount#NONE}, which will not wait for any shards
         * to be active. Set this value to {@link ActiveShardCount#DEFAULT} to wait for the primary
         * shard to be active. Set this value to {@link ActiveShardCount#ALL} to  wait for all shards
         * (primary and all replicas) to be active before returning.
         *
         * @param waitForActiveShards number of active shard copies to wait on
         */
        public void waitForActiveShards(ActiveShardCount waitForActiveShards) {
            if (waitForActiveShards.equals(ActiveShardCount.DEFAULT)) {
                this.waitForActiveShards = ActiveShardCount.NONE;
            } else {
                this.waitForActiveShards = waitForActiveShards;
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = parameters.validate();
            if (remoteCluster == null) {
                e = addValidationError(REMOTE_CLUSTER_FIELD.getPreferredName() + " is missing", e);
            }
            if (leaderIndex == null) {
                e = addValidationError(LEADER_INDEX_FIELD.getPreferredName() + " is missing", e);
            }
            if (followerIndex == null) {
                e = addValidationError("follower_index is missing", e);
            }
            if (dataStreamName != null && Strings.hasText(dataStreamName) == false) {
                e = addValidationError("data stream name must contain text if present", e);
            }
            return e;
        }

        @Override
        public String[] indices() {
            return new String[] { followerIndex };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.remoteCluster = in.readString();
            this.leaderIndex = in.readString();
            this.followerIndex = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_9_0)) {
                this.settings = Settings.readSettingsFromStream(in);
            }
            this.parameters = new FollowParameters(in);
            waitForActiveShards(ActiveShardCount.readFrom(in));
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                this.dataStreamName = in.readOptionalString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(remoteCluster);
            out.writeString(leaderIndex);
            out.writeString(followerIndex);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_9_0)) {
                settings.writeTo(out);
            }
            parameters.writeTo(out);
            waitForActiveShards.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
                out.writeOptionalString(this.dataStreamName);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
                builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
                if (dataStreamName != null) {
                    builder.field(DATA_STREAM_NAME.getPreferredName(), dataStreamName);
                }
                if (settings.isEmpty() == false) {
                    builder.startObject(SETTINGS_FIELD.getPreferredName());
                    {
                        settings.toXContent(builder, params);
                    }
                    builder.endObject();
                }
                parameters.toXContentFragment(builder);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(remoteCluster, request.remoteCluster)
                && Objects.equals(leaderIndex, request.leaderIndex)
                && Objects.equals(followerIndex, request.followerIndex)
                && Objects.equals(parameters, request.parameters)
                && Objects.equals(waitForActiveShards, request.waitForActiveShards)
                && Objects.equals(dataStreamName, request.dataStreamName)
                && Objects.equals(settings, request.settings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(remoteCluster, leaderIndex, followerIndex, parameters, settings, waitForActiveShards, dataStreamName);
        }

        // This class only exists for reuse of the FollowParameters class, see comment above the parser field.
        private static class PutFollowParameters extends FollowParameters {

            private String remoteCluster;
            private String leaderIndex;
            private String dataStreamName;
            private Settings settings = Settings.EMPTY;

        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final boolean followIndexCreated;
        private final boolean followIndexShardsAcked;
        private final boolean indexFollowingStarted;

        public Response(boolean followIndexCreated, boolean followIndexShardsAcked, boolean indexFollowingStarted) {
            this.followIndexCreated = followIndexCreated;
            this.followIndexShardsAcked = followIndexShardsAcked;
            this.indexFollowingStarted = indexFollowingStarted;
        }

        public boolean isFollowIndexCreated() {
            return followIndexCreated;
        }

        public boolean isFollowIndexShardsAcked() {
            return followIndexShardsAcked;
        }

        public boolean isIndexFollowingStarted() {
            return indexFollowingStarted;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            followIndexCreated = in.readBoolean();
            followIndexShardsAcked = in.readBoolean();
            indexFollowingStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(followIndexCreated);
            out.writeBoolean(followIndexShardsAcked);
            out.writeBoolean(indexFollowingStarted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("follow_index_created", followIndexCreated);
                builder.field("follow_index_shards_acked", followIndexShardsAcked);
                builder.field("index_following_started", indexFollowingStarted);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return followIndexCreated == response.followIndexCreated
                && followIndexShardsAcked == response.followIndexShardsAcked
                && indexFollowingStarted == response.indexFollowingStarted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
        }

        @Override
        public String toString() {
            return "PutFollowAction.Response{"
                + "followIndexCreated="
                + followIndexCreated
                + ", followIndexShardsAcked="
                + followIndexShardsAcked
                + ", indexFollowingStarted="
                + indexFollowingStarted
                + '}';
        }
    }

}
