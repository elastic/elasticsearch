/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_OUTSTANDING_READ_REQUESTS;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_OUTSTANDING_WRITE_REQUESTS;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_READ_REQUEST_OPERATION_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_READ_REQUEST_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_RETRY_DELAY_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_BUFFER_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_REQUEST_OPERATION_COUNT;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.MAX_WRITE_REQUEST_SIZE;
import static org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction.Request.READ_POLL_TIMEOUT;

public class FollowInfoAction extends Action<FollowInfoAction.Request, FollowInfoAction.Response, FollowInfoAction.RequestBuilder> {

    public static final String NAME = "cluster:monitor/ccr/follow_info";

    public static final FollowInfoAction INSTANCE = new FollowInfoAction();

    private FollowInfoAction() {
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

    public static class Request extends MasterNodeReadRequest<Request> {

        private String[] followerIndices;

        public Request() {
        }

        public String[] getFollowerIndices() {
            return followerIndices;
        }

        public void setFollowerIndices(String... followerIndices) {
            this.followerIndices = followerIndices;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            followerIndices = in.readOptionalStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(followerIndices);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(followerIndices, request.followerIndices);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(followerIndices);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField FOLLOWER_INDICES_FIELD = new ParseField("follower_indices");

        private final List<FollowerInfo> followInfos;

        public Response(List<FollowerInfo> followInfos) {
            this.followInfos = followInfos;
        }

        public List<FollowerInfo> getFollowInfos() {
            return followInfos;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            followInfos = in.readList(FollowerInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(followInfos);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(FOLLOWER_INDICES_FIELD.getPreferredName());
            for (FollowerInfo followInfo : followInfos) {
                followInfo.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(followInfos, response.followInfos);
        }

        @Override
        public int hashCode() {
            return Objects.hash(followInfos);
        }

        public String toString() {
            return Strings.toString(this);
        }

        public static class FollowerInfo implements Writeable, ToXContentObject {

            public static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
            public static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
            public static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
            public static final ParseField STATUS_FIELD = new ParseField("status");
            public static final ParseField PARAMETERS_FIELD = new ParseField("parameters");

            private final String followerIndex;
            private final String remoteCluster;
            private final String leaderIndex;
            private final Status status;
            private final FollowParameters parameters;

            public FollowerInfo(String followerIndex, String remoteCluster, String leaderIndex, Status status,
                                FollowParameters parameters) {
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

            public FollowParameters getParameters() {
                return parameters;
            }

            FollowerInfo(StreamInput in) throws IOException {
                followerIndex = in.readString();
                remoteCluster = in.readString();
                leaderIndex = in.readString();
                status = Status.fromString(in.readString());
                parameters = in.readOptionalWriteable(FollowParameters::new);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(followerIndex);
                out.writeString(remoteCluster);
                out.writeString(leaderIndex);
                out.writeString(status.name);
                out.writeOptionalWriteable(parameters);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
                builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
                builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
                builder.field(STATUS_FIELD.getPreferredName(), status.name);
                if (parameters != null) {
                    builder.startObject(PARAMETERS_FIELD.getPreferredName());
                    {
                        builder.field(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), parameters.maxReadRequestOperationCount);
                        builder.field(MAX_READ_REQUEST_SIZE.getPreferredName(), parameters.maxReadRequestSize.getStringRep());
                        builder.field(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), parameters.maxOutstandingReadRequests);
                        builder.field(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), parameters.maxWriteRequestOperationCount);
                        builder.field(MAX_WRITE_REQUEST_SIZE.getPreferredName(), parameters.maxWriteRequestSize.getStringRep());
                        builder.field(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), parameters.maxOutstandingWriteRequests);
                        builder.field(MAX_WRITE_BUFFER_COUNT.getPreferredName(), parameters.maxWriteBufferCount);
                        builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), parameters.maxWriteBufferSize.getStringRep());
                        builder.field(MAX_RETRY_DELAY_FIELD.getPreferredName(), parameters.maxRetryDelay.getStringRep());
                        builder.field(READ_POLL_TIMEOUT.getPreferredName(), parameters.readPollTimeout.getStringRep());
                    }
                    builder.endObject();
                }
                builder.endObject();
                return builder;
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

            public String toString() {
                return Strings.toString(this);
            }
        }

        public static class FollowParameters implements Writeable {

            private final int maxReadRequestOperationCount;
            private final ByteSizeValue maxReadRequestSize;
            private final int maxOutstandingReadRequests;
            private final int maxWriteRequestOperationCount;
            private final ByteSizeValue maxWriteRequestSize;
            private final int maxOutstandingWriteRequests;
            private final int maxWriteBufferCount;
            private final ByteSizeValue maxWriteBufferSize;
            private final TimeValue maxRetryDelay;
            private final TimeValue readPollTimeout;

            public FollowParameters(int maxReadRequestOperationCount,
                                    ByteSizeValue maxReadRequestSize, int maxOutstandingReadRequests,
                                    int maxWriteRequestOperationCount, ByteSizeValue maxWriteRequestSize,
                                    int maxOutstandingWriteRequests, int maxWriteBufferCount,
                                    ByteSizeValue maxWriteBufferSize, TimeValue maxRetryDelay, TimeValue readPollTimeout) {
                this.maxReadRequestOperationCount = maxReadRequestOperationCount;
                this.maxReadRequestSize = maxReadRequestSize;
                this.maxOutstandingReadRequests = maxOutstandingReadRequests;
                this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
                this.maxWriteRequestSize = maxWriteRequestSize;
                this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
                this.maxWriteBufferCount = maxWriteBufferCount;
                this.maxWriteBufferSize = maxWriteBufferSize;
                this.maxRetryDelay = maxRetryDelay;
                this.readPollTimeout = readPollTimeout;
            }

            public int getMaxReadRequestOperationCount() {
                return maxReadRequestOperationCount;
            }

            public ByteSizeValue getMaxReadRequestSize() {
                return maxReadRequestSize;
            }

            public int getMaxOutstandingReadRequests() {
                return maxOutstandingReadRequests;
            }

            public int getMaxWriteRequestOperationCount() {
                return maxWriteRequestOperationCount;
            }

            public ByteSizeValue getMaxWriteRequestSize() {
                return maxWriteRequestSize;
            }

            public int getMaxOutstandingWriteRequests() {
                return maxOutstandingWriteRequests;
            }

            public int getMaxWriteBufferCount() {
                return maxWriteBufferCount;
            }

            public ByteSizeValue getMaxWriteBufferSize() {
                return maxWriteBufferSize;
            }

            public TimeValue getMaxRetryDelay() {
                return maxRetryDelay;
            }

            public TimeValue getReadPollTimeout() {
                return readPollTimeout;
            }

            FollowParameters(StreamInput in) throws IOException {
                this.maxReadRequestOperationCount = in.readVInt();
                this.maxReadRequestSize = new ByteSizeValue(in);
                this.maxOutstandingReadRequests = in.readVInt();
                this.maxWriteRequestOperationCount = in.readVInt();
                this.maxWriteRequestSize = new ByteSizeValue(in);
                this.maxOutstandingWriteRequests = in.readVInt();
                this.maxWriteBufferCount = in.readVInt();
                this.maxWriteBufferSize = new ByteSizeValue(in);
                this.maxRetryDelay = in.readTimeValue();
                this.readPollTimeout = in.readTimeValue();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVLong(maxReadRequestOperationCount);
                maxReadRequestSize.writeTo(out);
                out.writeVInt(maxOutstandingReadRequests);
                out.writeVLong(maxWriteRequestOperationCount);
                maxWriteRequestSize.writeTo(out);
                out.writeVInt(maxOutstandingWriteRequests);
                out.writeVInt(maxWriteBufferCount);
                maxWriteBufferSize.writeTo(out);
                out.writeTimeValue(maxRetryDelay);
                out.writeTimeValue(readPollTimeout);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                FollowParameters that = (FollowParameters) o;
                return maxReadRequestOperationCount == that.maxReadRequestOperationCount &&
                    maxOutstandingReadRequests == that.maxOutstandingReadRequests &&
                    maxWriteRequestOperationCount == that.maxWriteRequestOperationCount &&
                    maxOutstandingWriteRequests == that.maxOutstandingWriteRequests &&
                    maxWriteBufferCount == that.maxWriteBufferCount &&
                    Objects.equals(maxReadRequestSize, that.maxReadRequestSize) &&
                    Objects.equals(maxWriteRequestSize, that.maxWriteRequestSize) &&
                    Objects.equals(maxWriteBufferSize, that.maxWriteBufferSize) &&
                    Objects.equals(maxRetryDelay, that.maxRetryDelay) &&
                    Objects.equals(readPollTimeout, that.readPollTimeout);
            }

            @Override
            public int hashCode() {
                return Objects.hash(
                    maxReadRequestOperationCount,
                    maxReadRequestSize,
                    maxOutstandingReadRequests,
                    maxWriteRequestOperationCount,
                    maxWriteRequestSize,
                    maxOutstandingWriteRequests,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    readPollTimeout
                );
            }

        }

        public enum Status {

            ACTIVE("active"),
            PAUSED("paused");

            private final String name;

            Status(String name) {
                this.name = name;
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

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(
            final ElasticsearchClient client,
            final Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }

    }

}
