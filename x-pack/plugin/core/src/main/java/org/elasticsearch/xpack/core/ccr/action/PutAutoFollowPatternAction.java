/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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

        private static final ObjectParser<Request, String> PARSER = new ObjectParser<>("put_auto_follow_pattern_request", Request::new);
        private static final ParseField NAME_FIELD = new ParseField("name");
        private static final int MAX_NAME_BYTES = 255;

        static {
            PARSER.declareString(Request::setName, NAME_FIELD);
            PARSER.declareString(Request::setRemoteCluster, REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(Request::setLeaderIndexPatterns, AutoFollowPattern.LEADER_PATTERNS_FIELD);
            PARSER.declareString(Request::setFollowIndexNamePattern, AutoFollowPattern.FOLLOW_PATTERN_FIELD);
            PARSER.declareInt(Request::setMaxReadRequestOperationCount, AutoFollowPattern.MAX_READ_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                    Request::setMaxReadRequestSize,
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), AutoFollowPattern.MAX_READ_REQUEST_SIZE.getPreferredName()),
                    AutoFollowPattern.MAX_READ_REQUEST_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareInt(Request::setMaxConcurrentReadBatches, AutoFollowPattern.MAX_OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt(Request::setMaxWriteRequestOperationCount, AutoFollowPattern.MAX_WRITE_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                Request::setMaxWriteRequestSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), AutoFollowPattern.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
                AutoFollowPattern.MAX_WRITE_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(Request::setMaxConcurrentWriteBatches, AutoFollowPattern.MAX_OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt(Request::setMaxWriteBufferCount, AutoFollowPattern.MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                Request::setMaxWriteBufferSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), AutoFollowPattern.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                AutoFollowPattern.MAX_WRITE_BUFFER_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(Request::setMaxRetryDelay,
                (p, c) -> TimeValue.parseTimeValue(p.text(), AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName()),
                AutoFollowPattern.MAX_RETRY_DELAY, ObjectParser.ValueType.STRING);
            PARSER.declareField(Request::setReadPollTimeout,
                (p, c) -> TimeValue.parseTimeValue(p.text(), AutoFollowPattern.READ_POLL_TIMEOUT.getPreferredName()),
                AutoFollowPattern.READ_POLL_TIMEOUT, ObjectParser.ValueType.STRING);
        }

        public static Request fromXContent(XContentParser parser, String name) throws IOException {
            Request request = PARSER.parse(parser, null);
            if (name != null) {
                if (request.name == null) {
                    request.name = name;
                } else {
                    if (request.name.equals(name) == false) {
                        throw new IllegalArgumentException("provided name is not equal");
                    }
                }
            }
            return request;
        }

        private String name;
        private String remoteCluster;
        private List<String> leaderIndexPatterns;
        private String followIndexNamePattern;

        private Integer maxReadRequestOperationCount;
        private ByteSizeValue maxReadRequestSize;
        private Integer maxConcurrentReadBatches;
        private Integer maxWriteRequestOperationCount;
        private ByteSizeValue maxWriteRequestSize;
        private Integer maxConcurrentWriteBatches;
        private Integer maxWriteBufferCount;
        private ByteSizeValue maxWriteBufferSize;
        private TimeValue maxRetryDelay;
        private TimeValue readPollTimeout;

        public Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
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
            if (maxRetryDelay != null) {
                if (maxRetryDelay.millis() <= 0) {
                    String message = "[" + AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName() + "] must be positive but was [" +
                        maxRetryDelay.getStringRep() + "]";
                    validationException = addValidationError(message, validationException);
                }
                if (maxRetryDelay.millis() > ResumeFollowAction.MAX_RETRY_DELAY.millis()) {
                    String message = "[" + AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName() + "] must be less than [" +
                        ResumeFollowAction.MAX_RETRY_DELAY +
                        "] but was [" + maxRetryDelay.getStringRep() + "]";
                    validationException = addValidationError(message, validationException);
                }
            }
            return validationException;
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

        public Integer getMaxReadRequestOperationCount() {
            return maxReadRequestOperationCount;
        }

        public void setMaxReadRequestOperationCount(Integer maxReadRequestOperationCount) {
            this.maxReadRequestOperationCount = maxReadRequestOperationCount;
        }

        public Integer getMaxConcurrentReadBatches() {
            return maxConcurrentReadBatches;
        }

        public void setMaxConcurrentReadBatches(Integer maxConcurrentReadBatches) {
            this.maxConcurrentReadBatches = maxConcurrentReadBatches;
        }

        public ByteSizeValue getMaxReadRequestSize() {
            return maxReadRequestSize;
        }

        public void setMaxReadRequestSize(ByteSizeValue maxReadRequestSize) {
            this.maxReadRequestSize = maxReadRequestSize;
        }

        public Integer getMaxWriteRequestOperationCount() {
            return maxWriteRequestOperationCount;
        }

        public void setMaxWriteRequestOperationCount(Integer maxWriteRequestOperationCount) {
            this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
        }

        public ByteSizeValue getMaxWriteRequestSize() {
            return maxWriteRequestSize;
        }

        public void setMaxWriteRequestSize(ByteSizeValue maxWriteRequestSize) {
            this.maxWriteRequestSize = maxWriteRequestSize;
        }

        public Integer getMaxConcurrentWriteBatches() {
            return maxConcurrentWriteBatches;
        }

        public void setMaxConcurrentWriteBatches(Integer maxConcurrentWriteBatches) {
            this.maxConcurrentWriteBatches = maxConcurrentWriteBatches;
        }

        public Integer getMaxWriteBufferCount() {
            return maxWriteBufferCount;
        }

        public void setMaxWriteBufferCount(Integer maxWriteBufferCount) {
            this.maxWriteBufferCount = maxWriteBufferCount;
        }

        public ByteSizeValue getMaxWriteBufferSize() {
            return maxWriteBufferSize;
        }

        public void setMaxWriteBufferSize(ByteSizeValue maxWriteBufferSize) {
            this.maxWriteBufferSize = maxWriteBufferSize;
        }

        public TimeValue getMaxRetryDelay() {
            return maxRetryDelay;
        }

        public void setMaxRetryDelay(TimeValue maxRetryDelay) {
            this.maxRetryDelay = maxRetryDelay;
        }

        public TimeValue getReadPollTimeout() {
            return readPollTimeout;
        }

        public void setReadPollTimeout(TimeValue readPollTimeout) {
            this.readPollTimeout = readPollTimeout;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            remoteCluster = in.readString();
            leaderIndexPatterns = in.readList(StreamInput::readString);
            followIndexNamePattern = in.readOptionalString();
            maxReadRequestOperationCount = in.readOptionalVInt();
            maxReadRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxConcurrentReadBatches = in.readOptionalVInt();
            maxWriteRequestOperationCount = in.readOptionalVInt();
            maxWriteRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxConcurrentWriteBatches = in.readOptionalVInt();
            maxWriteBufferCount = in.readOptionalVInt();
            maxWriteBufferSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxRetryDelay = in.readOptionalTimeValue();
            readPollTimeout = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(remoteCluster);
            out.writeStringList(leaderIndexPatterns);
            out.writeOptionalString(followIndexNamePattern);
            out.writeOptionalVInt(maxReadRequestOperationCount);
            out.writeOptionalWriteable(maxReadRequestSize);
            out.writeOptionalVInt(maxConcurrentReadBatches);
            out.writeOptionalVInt(maxWriteRequestOperationCount);
            out.writeOptionalWriteable(maxWriteRequestSize);
            out.writeOptionalVInt(maxConcurrentWriteBatches);
            out.writeOptionalVInt(maxWriteBufferCount);
            out.writeOptionalWriteable(maxWriteBufferSize);
            out.writeOptionalTimeValue(maxRetryDelay);
            out.writeOptionalTimeValue(readPollTimeout);
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
                if (maxReadRequestOperationCount != null) {
                    builder.field(AutoFollowPattern.MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
                }
                if (maxReadRequestSize != null) {
                    builder.field(AutoFollowPattern.MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
                }
                if (maxWriteRequestOperationCount != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
                }
                if (maxWriteRequestSize != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_REQUEST_SIZE.getPreferredName(), maxWriteRequestSize.getStringRep());
                }
                if (maxWriteBufferCount != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
                }
                if (maxWriteBufferSize != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
                }
                if (maxConcurrentReadBatches != null) {
                    builder.field(AutoFollowPattern.MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxConcurrentReadBatches);
                }
                if (maxConcurrentWriteBatches != null) {
                    builder.field(AutoFollowPattern.MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxConcurrentWriteBatches);
                }
                if (maxRetryDelay != null) {
                    builder.field(AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
                }
                if (readPollTimeout != null) {
                    builder.field(AutoFollowPattern.READ_POLL_TIMEOUT.getPreferredName(), readPollTimeout.getStringRep());
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name) &&
                    Objects.equals(remoteCluster, request.remoteCluster) &&
                    Objects.equals(leaderIndexPatterns, request.leaderIndexPatterns) &&
                    Objects.equals(followIndexNamePattern, request.followIndexNamePattern) &&
                    Objects.equals(maxReadRequestOperationCount, request.maxReadRequestOperationCount) &&
                    Objects.equals(maxReadRequestSize, request.maxReadRequestSize) &&
                    Objects.equals(maxConcurrentReadBatches, request.maxConcurrentReadBatches) &&
                    Objects.equals(maxWriteRequestOperationCount, request.maxWriteRequestOperationCount) &&
                    Objects.equals(maxWriteRequestSize, request.maxWriteRequestSize) &&
                    Objects.equals(maxConcurrentWriteBatches, request.maxConcurrentWriteBatches) &&
                    Objects.equals(maxWriteBufferCount, request.maxWriteBufferCount) &&
                    Objects.equals(maxWriteBufferSize, request.maxWriteBufferSize) &&
                    Objects.equals(maxRetryDelay, request.maxRetryDelay) &&
                    Objects.equals(readPollTimeout, request.readPollTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    name,
                    remoteCluster,
                    leaderIndexPatterns,
                    followIndexNamePattern,
                    maxReadRequestOperationCount,
                    maxReadRequestSize,
                    maxConcurrentReadBatches,
                    maxWriteRequestOperationCount,
                    maxWriteRequestSize,
                    maxConcurrentWriteBatches,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    readPollTimeout);
        }
    }

}
