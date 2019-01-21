/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class ResumeFollowAction extends Action<AcknowledgedResponse> {

    public static final ResumeFollowAction INSTANCE = new ResumeFollowAction();
    public static final String NAME = "cluster:admin/xpack/ccr/resume_follow";

    private ResumeFollowAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");

        public static Request fromXContent(final XContentParser parser, final String followerIndex) throws IOException {
            Body body = Body.PARSER.parse(parser, null);
            if (followerIndex != null) {
                if (body.followerIndex == null) {
                    body.followerIndex = followerIndex;
                } else {
                    if (body.followerIndex.equals(followerIndex) == false) {
                        throw new IllegalArgumentException("provided follower_index is not equal");
                    }
                }
            }
            Request request = new Request();
            request.setBody(body);
            return request;
        }

        private Body body = new Body();

        public Request() {
        }

        public Body getBody() {
            return body;
        }

        public void setBody(Body body) {
            this.body = body;
        }

        @Override
        public ActionRequestValidationException validate() {
            return body.validate();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            body = new Body(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            body.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return body.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(body, request.body);
        }

        @Override
        public int hashCode() {
            return Objects.hash(body);
        }

        public static class Body extends FollowParameters implements ToXContentObject {

            static final ObjectParser<Body, Void> PARSER = new ObjectParser<>(NAME, Body::new);

            static {
                PARSER.declareString(Body::setFollowerIndex, FOLLOWER_INDEX_FIELD);
                initParser(PARSER);
            }

            private String followerIndex;

            public Body() {
            }

            public String getFollowerIndex() {
                return followerIndex;
            }

            public void setFollowerIndex(String followerIndex) {
                this.followerIndex = followerIndex;
            }

            @Override
            public ActionRequestValidationException validate() {
                ActionRequestValidationException e = super.validate();
                if (followerIndex == null) {
                    e = addValidationError(FOLLOWER_INDEX_FIELD.getPreferredName() + " is missing", e);
                }
                return e;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                {
                    builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
                    super.toXContentFragment(builder);
                }
                builder.endObject();
                return builder;
            }

            Body(StreamInput in) throws IOException {
                followerIndex = in.readString();
                fromStreamInput(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(followerIndex);
                super.writeTo(out);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                Body body = (Body) o;
                return Objects.equals(followerIndex, body.followerIndex);
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), followerIndex);
            }
        }
    }

}
