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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class CreateAndFollowIndexAction extends Action<CreateAndFollowIndexAction.Response> {

    public static final CreateAndFollowIndexAction INSTANCE = new CreateAndFollowIndexAction();
    public static final String NAME = "indices:admin/xpack/ccr/create_and_follow_index";

    private CreateAndFollowIndexAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private FollowIndexAction.Request followRequest;

        public Request(FollowIndexAction.Request followRequest) {
            this.followRequest = Objects.requireNonNull(followRequest);
        }

        public Request() {

        }

        public FollowIndexAction.Request getFollowRequest() {
            return followRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return followRequest.validate();
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
            followRequest = new FollowIndexAction.Request();
            followRequest.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            followRequest.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(followRequest, request.followRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(followRequest);
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
