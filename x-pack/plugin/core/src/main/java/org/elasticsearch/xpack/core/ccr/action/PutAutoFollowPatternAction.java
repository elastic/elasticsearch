/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern.REMOTE_CLUSTER_FIELD;

public class PutAutoFollowPatternAction extends Action<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/xpack/ccr/auto_follow_pattern/put";
    public static final PutAutoFollowPatternAction INSTANCE = new PutAutoFollowPatternAction();

    private PutAutoFollowPatternAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static Request fromXContent(XContentParser parser, String name) throws IOException {
            Body body = Body.PARSER.parse(parser, null);
            if (name != null) {
                if (body.name == null) {
                    body.name = name;
                } else {
                    if (body.name.equals(name) == false) {
                        throw new IllegalArgumentException("provided name is not equal");
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

        @Override
        public ActionRequestValidationException validate() {
            return body.validate();
        }

        public Body getBody() {
            return body;
        }

        public void setBody(Body body) {
            this.body = body;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.body = new Body(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            body.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

            private static final ObjectParser<Body, Void> PARSER = new ObjectParser<>("put_auto_follow_pattern_request", Body::new);
            private static final ParseField NAME_FIELD = new ParseField("name");
            private static final int MAX_NAME_BYTES = 255;

            static {
                PARSER.declareString(Body::setName, NAME_FIELD);
                PARSER.declareString(Body::setRemoteCluster, REMOTE_CLUSTER_FIELD);
                PARSER.declareStringArray(Body::setLeaderIndexPatterns, AutoFollowPattern.LEADER_PATTERNS_FIELD);
                PARSER.declareString(Body::setFollowIndexNamePattern, AutoFollowPattern.FOLLOW_PATTERN_FIELD);
                initParser(PARSER);
            }

            private String name;
            private String remoteCluster;
            private List<String> leaderIndexPatterns;
            private String followIndexNamePattern;

            public Body() {
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getRemoteCluster() {
                return remoteCluster;
            }

            public void setRemoteCluster(String remoteCluster) {
                this.remoteCluster = remoteCluster;
            }

            public List<String> getLeaderIndexPatterns() {
                return leaderIndexPatterns;
            }

            public void setLeaderIndexPatterns(List<String> leaderIndexPatterns) {
                this.leaderIndexPatterns = leaderIndexPatterns;
            }

            public String getFollowIndexNamePattern() {
                return followIndexNamePattern;
            }

            public void setFollowIndexNamePattern(String followIndexNamePattern) {
                this.followIndexNamePattern = followIndexNamePattern;
            }

            @Override
            public ActionRequestValidationException validate() {
                ActionRequestValidationException validationException = super.validate();
                if (name == null) {
                    validationException = addValidationError("[" + NAME_FIELD.getPreferredName() + "] is missing", validationException);
                }
                if (name != null) {
                    if (name.contains(",")) {
                        validationException = addValidationError("[" + NAME_FIELD.getPreferredName() + "] name must not contain a ','",
                            validationException);
                    }
                    if (name.startsWith("_")) {
                        validationException = addValidationError("[" + NAME_FIELD.getPreferredName() + "] name must not start with '_'",
                            validationException);
                    }
                    int byteCount = name.getBytes(StandardCharsets.UTF_8).length;
                    if (byteCount > MAX_NAME_BYTES) {
                        validationException = addValidationError("[" + NAME_FIELD.getPreferredName() + "] name is too long (" +
                            byteCount + " > " + MAX_NAME_BYTES + ")", validationException);
                    }
                }
                if (remoteCluster == null) {
                    validationException = addValidationError("[" + REMOTE_CLUSTER_FIELD.getPreferredName() +
                        "] is missing", validationException);
                }
                if (leaderIndexPatterns == null || leaderIndexPatterns.isEmpty()) {
                    validationException = addValidationError("[" + AutoFollowPattern.LEADER_PATTERNS_FIELD.getPreferredName() +
                        "] is missing", validationException);
                }
                return validationException;
            }

            Body(StreamInput in) throws IOException {
                name = in.readString();
                remoteCluster = in.readString();
                leaderIndexPatterns = in.readList(StreamInput::readString);
                followIndexNamePattern = in.readOptionalString();
                if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
                    fromStreamInput(in);
                } else {
                    maxReadRequestOperationCount = in.readOptionalVInt();
                    maxReadRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
                    maxOutstandingReadRequests = in.readOptionalVInt();
                    maxWriteRequestOperationCount = in.readOptionalVInt();
                    maxWriteRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
                    maxOutstandingWriteRequests = in.readOptionalVInt();
                    maxWriteBufferCount = in.readOptionalVInt();
                    maxWriteBufferSize = in.readOptionalWriteable(ByteSizeValue::new);
                    maxRetryDelay = in.readOptionalTimeValue();
                    readPollTimeout = in.readOptionalTimeValue();
                }
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeString(remoteCluster);
                out.writeStringList(leaderIndexPatterns);
                out.writeOptionalString(followIndexNamePattern);
                if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
                    super.writeTo(out);
                } else {
                    out.writeOptionalVInt(maxReadRequestOperationCount);
                    out.writeOptionalWriteable(maxReadRequestSize);
                    out.writeOptionalVInt(maxOutstandingReadRequests);
                    out.writeOptionalVInt(maxWriteRequestOperationCount);
                    out.writeOptionalWriteable(maxWriteRequestSize);
                    out.writeOptionalVInt(maxOutstandingWriteRequests);
                    out.writeOptionalVInt(maxWriteBufferCount);
                    out.writeOptionalWriteable(maxWriteBufferSize);
                    out.writeOptionalTimeValue(maxRetryDelay);
                    out.writeOptionalTimeValue(readPollTimeout);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                {
                    builder.field(NAME_FIELD.getPreferredName(), name);
                    builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
                    builder.field(AutoFollowPattern.LEADER_PATTERNS_FIELD.getPreferredName(), leaderIndexPatterns);
                    if (followIndexNamePattern != null) {
                        builder.field(AutoFollowPattern.FOLLOW_PATTERN_FIELD.getPreferredName(), followIndexNamePattern);
                    }
                    toXContentFragment(builder);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                Body body = (Body) o;
                return Objects.equals(name, body.name) &&
                    Objects.equals(remoteCluster, body.remoteCluster) &&
                    Objects.equals(leaderIndexPatterns, body.leaderIndexPatterns) &&
                    Objects.equals(followIndexNamePattern, body.followIndexNamePattern);
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), name, remoteCluster, leaderIndexPatterns, followIndexNamePattern);
            }
        }

    }

}
