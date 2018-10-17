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
import java.util.Map;
import java.util.Objects;

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
            this.leaderClusterAlias = in.readOptionalString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
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
            out.writeOptionalString(leaderClusterAlias);
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

        private Map<String, AutoFollowPattern> autoFollowPatterns;

        public Response(Map<String, AutoFollowPattern> autoFollowPatterns) {
            this.autoFollowPatterns = autoFollowPatterns;
        }

        public Response() {
        }

        public Map<String, AutoFollowPattern> getAutoFollowPatterns() {
            return autoFollowPatterns;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            autoFollowPatterns = in.readMap(StreamInput::readString, AutoFollowPattern::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(autoFollowPatterns, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (Map.Entry<String, AutoFollowPattern> entry : autoFollowPatterns.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(autoFollowPatterns, response.autoFollowPatterns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(autoFollowPatterns);
        }
    }

}
