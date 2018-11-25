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
import org.elasticsearch.common.io.stream.Writeable;
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
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_READ_REQUEST_OPERATION_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_READ_REQUEST_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_OUTSTANDING_READ_REQUESTS;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_OUTSTANDING_WRITE_REQUESTS;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_RETRY_DELAY_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_REQUEST_OPERATION_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_REQUEST_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.READ_POLL_TIMEOUT;

public final class PutFollowAction extends Action<PutFollowAction.Response> {

    public static final PutFollowAction INSTANCE = new PutFollowAction();
    public static final String NAME = "indices:admin/xpack/ccr/put_follow";

    private PutFollowAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
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
            PARSER.declareString((req, val) -> req.followRequest.setFollowerIndex(val), FOLLOWER_INDEX_FIELD);
            PARSER.declareInt((req, val) -> req.followRequest.setMaxReadRequestOperationCount(val), MAX_READ_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                (req, val) -> req.followRequest.setMaxReadRequestSize(val),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_READ_REQUEST_SIZE.getPreferredName()),
                MAX_READ_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt((req, val) -> req.followRequest.setMaxOutstandingReadRequests(val), MAX_OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt((req, val) -> req.followRequest.setMaxWriteRequestOperationCount(val), MAX_WRITE_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                    (req, val) -> req.followRequest.setMaxWriteRequestSize(val),
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_REQUEST_SIZE.getPreferredName()),
                    MAX_WRITE_REQUEST_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareInt((req, val) -> req.followRequest.setMaxOutstandingWriteRequests(val), MAX_OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt((req, val) -> req.followRequest.setMaxWriteBufferCount(val), MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                (req, val) -> req.followRequest.setMaxWriteBufferSize(val),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                MAX_WRITE_BUFFER_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                (req, val) -> req.followRequest.setMaxRetryDelay(val),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY_FIELD.getPreferredName()),
                MAX_RETRY_DELAY_FIELD,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                (req, val) -> req.followRequest.setReadPollTimeout(val),
                (p, c) -> TimeValue.parseTimeValue(p.text(), READ_POLL_TIMEOUT.getPreferredName()),
                READ_POLL_TIMEOUT,
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

        public Request(StreamInput in) throws IOException {
            super(in);
            remoteCluster = in.readString();
            leaderIndex = in.readString();
            followRequest = new ResumeFollowAction.Request(in);
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
