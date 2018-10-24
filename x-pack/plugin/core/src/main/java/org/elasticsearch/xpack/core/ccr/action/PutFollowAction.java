/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.FOLLOWER_INDEX_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_BATCH_OPERATION_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_CONCURRENT_READ_BATCHES;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_CONCURRENT_WRITE_BATCHES;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_RETRY_DELAY_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.POLL_TIMEOUT;

public final class PutFollowAction extends Action<PutFollowAction.Response> {

    public static final PutFollowAction INSTANCE = new PutFollowAction();
    public static final String NAME = "indices:admin/xpack/ccr/put_follow";

    private PutFollowAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        private static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        private static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");

        private static final ObjectParser<Request, String> PARSER = new ObjectParser<>(NAME, () -> {
            Request request = new Request();
            request.setFollowRequest(new ResumeFollowAction.Request());
            return request;
        });

        static {
            PARSER.declareString(Request::setRemoteCluster, REMOTE_CLUSTER_FIELD);
            PARSER.declareString(Request::setLeaderIndex, LEADER_INDEX_FIELD);
            PARSER.declareString((request, value) -> request.followRequest.setFollowerIndex(value), FOLLOWER_INDEX_FIELD);
            PARSER.declareInt((request, value) -> request.followRequest.setMaxBatchOperationCount(value), MAX_BATCH_OPERATION_COUNT);
            PARSER.declareInt((request, value) -> request.followRequest.setMaxConcurrentReadBatches(value), MAX_CONCURRENT_READ_BATCHES);
            PARSER.declareField(
                (request, value) -> request.followRequest.setMaxBatchSize(value),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_BATCH_SIZE.getPreferredName()),
                MAX_BATCH_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt((request, value) -> request.followRequest.setMaxConcurrentWriteBatches(value), MAX_CONCURRENT_WRITE_BATCHES);
            PARSER.declareInt((request, value) -> request.followRequest.setMaxWriteBufferCount(value), MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                (request, value) -> request.followRequest.setMaxWriteBufferSize(value),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                MAX_WRITE_BUFFER_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                (request, value) -> request.followRequest.setMaxRetryDelay(value),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY_FIELD.getPreferredName()),
                MAX_RETRY_DELAY_FIELD,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                (request, value) -> request.followRequest.setPollTimeout(value),
                (p, c) -> TimeValue.parseTimeValue(p.text(), POLL_TIMEOUT.getPreferredName()),
                POLL_TIMEOUT,
                ObjectParser.ValueType.STRING);
        }

        public static Request fromXContent(final XContentParser parser, final String followerIndex) throws IOException {
            Request request = PARSER.parse(parser, followerIndex);
            if (followerIndex != null) {
                if (request.getFollowRequest().getFollowerIndex() == null) {
                    request.getFollowRequest().setFollowerIndex(followerIndex);
                } else {
                    if (request.getFollowRequest().getFollowerIndex().equals(followerIndex) == false) {
                        throw new IllegalArgumentException("provided follower_index is not equal");
                    }
                }
            }
            return request;
        }

        private String remoteCluster;
        private String leaderIndex;
        private ResumeFollowAction.Request followRequest;

        public Request() {
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

        public ResumeFollowAction.Request getFollowRequest() {
            return followRequest;
        }

        public void setFollowRequest(ResumeFollowAction.Request followRequest) {
            this.followRequest = followRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = followRequest.validate();
            if (remoteCluster == null) {
                e = addValidationError(REMOTE_CLUSTER_FIELD.getPreferredName() + " is missing", e);
            }
            if (leaderIndex == null) {
                e = addValidationError(LEADER_INDEX_FIELD.getPreferredName() + " is missing", e);
            }
            return e;
        }

        @Override
        public String[] indices() {
            return new String[]{followRequest.getFollowerIndex()};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            remoteCluster = in.readString();
            leaderIndex = in.readString();
            followRequest = new ResumeFollowAction.Request();
            followRequest.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(remoteCluster);
            out.writeString(leaderIndex);
            followRequest.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
                builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
                followRequest.toXContentFragment(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(remoteCluster, request.remoteCluster) &&
                Objects.equals(leaderIndex, request.leaderIndex) &&
                Objects.equals(followRequest, request.followRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(remoteCluster, leaderIndex, followRequest);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean followIndexCreated;
        private boolean followIndexShardsAcked;
        private boolean indexFollowingStarted;

        public Response() {

        }

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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            followIndexCreated = in.readBoolean();
            followIndexShardsAcked = in.readBoolean();
            indexFollowingStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
            return followIndexCreated == response.followIndexCreated &&
                followIndexShardsAcked == response.followIndexShardsAcked &&
                indexFollowingStarted == response.indexFollowingStarted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
        }
    }

}
