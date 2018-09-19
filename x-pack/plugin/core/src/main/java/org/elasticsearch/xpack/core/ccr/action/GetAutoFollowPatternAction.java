/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetAutoFollowPatternAction extends Action<GetAutoFollowPatternAction.Response> {

    public static final String NAME = "cluster:admin/xpack/ccr/auto_follow_pattern/get";
    public static final GetAutoFollowPatternAction INSTANCE = new GetAutoFollowPatternAction();

    private GetAutoFollowPatternAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private String leaderClusterAlias;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.leaderClusterAlias = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (leaderClusterAlias == null) {
                validationException = addValidationError("leaderClusterAlias is missing", validationException);
            }
            return validationException;
        }

        public String getLeaderClusterAlias() {
            return leaderClusterAlias;
        }

        public void setLeaderClusterAlias(String leaderClusterAlias) {
            this.leaderClusterAlias = leaderClusterAlias;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderClusterAlias);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(leaderClusterAlias, request.leaderClusterAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leaderClusterAlias);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private AutoFollowPattern autoFollowPattern;

        public Response(AutoFollowPattern autoFollowPattern) {
            this.autoFollowPattern = autoFollowPattern;
        }

        public Response() {
        }

        public AutoFollowPattern getAutoFollowPattern() {
            return autoFollowPattern;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            autoFollowPattern = new AutoFollowPattern(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            autoFollowPattern.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            autoFollowPattern.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(autoFollowPattern, response.autoFollowPattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(autoFollowPattern);
        }
    }

}
