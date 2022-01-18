/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class ResumeFollowAction extends ActionType<AcknowledgedResponse> {

    public static final ResumeFollowAction INSTANCE = new ResumeFollowAction();
    public static final String NAME = "cluster:admin/xpack/ccr/resume_follow";

    private ResumeFollowAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        // Note that Request should be the Value class here for this parser with a 'parameters' field that maps to FollowParameters class
        // But since two minor version are already released with duplicate follow parameters in several APIs, FollowParameters
        // is now the Value class here.
        static final ObjectParser<FollowParameters, Void> PARSER = new ObjectParser<>(NAME, FollowParameters::new);

        static {
            FollowParameters.initParser(PARSER);
        }

        public static Request fromXContent(final XContentParser parser, final String followerIndex) throws IOException {
            FollowParameters parameters = PARSER.parse(parser, null);
            Request request = new Request();
            request.setFollowerIndex(followerIndex);
            request.setParameters(parameters);
            return request;
        }

        private String followerIndex;
        private FollowParameters parameters = new FollowParameters();

        public Request() {}

        public String getFollowerIndex() {
            return followerIndex;
        }

        public void setFollowerIndex(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        public FollowParameters getParameters() {
            return parameters;
        }

        public void setParameters(FollowParameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = parameters.validate();
            if (followerIndex == null) {
                e = addValidationError("follower_index is missing", e);
            }
            return e;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            followerIndex = in.readString();
            parameters = new FollowParameters(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerIndex);
            parameters.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
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
            return Objects.equals(followerIndex, request.followerIndex) && Objects.equals(parameters, request.parameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(followerIndex, parameters);
        }
    }

}
