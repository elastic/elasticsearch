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

        static {
            PARSER.declareString(Request::setName, NAME_FIELD);
            PARSER.declareString(Request::setRemoteCluster, REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(Request::setLeaderIndexPatterns, AutoFollowPattern.LEADER_PATTERNS_FIELD);
            PARSER.declareString(Request::setFollowIndexNamePattern, AutoFollowPattern.FOLLOW_PATTERN_FIELD);
            PARSER.declareInt(Request::setMaxBatchOperationCount, AutoFollowPattern.MAX_BATCH_OPERATION_COUNT);
            PARSER.declareInt(Request::setMaxConcurrentReadBatches, AutoFollowPattern.MAX_CONCURRENT_READ_BATCHES);
            PARSER.declareField(
                    Request::setMaxBatchSize,
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), AutoFollowPattern.MAX_BATCH_SIZE.getPreferredName()),
                    AutoFollowPattern.MAX_BATCH_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareInt(Request::setMaxConcurrentWriteBatches, AutoFollowPattern.MAX_CONCURRENT_WRITE_BATCHES);
            PARSER.declareInt(Request::setMaxWriteBufferCount, AutoFollowPattern.MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                Request::setMaxWriteBufferSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), AutoFollowPattern.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                AutoFollowPattern.MAX_WRITE_BUFFER_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(Request::setMaxRetryDelay,
                (p, c) -> TimeValue.parseTimeValue(p.text(), AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName()),
                AutoFollowPattern.MAX_RETRY_DELAY, ObjectParser.ValueType.STRING);
            PARSER.declareField(Request::setPollTimeout,
                (p, c) -> TimeValue.parseTimeValue(p.text(), AutoFollowPattern.POLL_TIMEOUT.getPreferredName()),
                AutoFollowPattern.POLL_TIMEOUT, ObjectParser.ValueType.STRING);
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

        private Integer maxBatchOperationCount;
        private Integer maxConcurrentReadBatches;
        private ByteSizeValue maxBatchSize;
        private Integer maxConcurrentWriteBatches;
        private Integer maxWriteBufferCount;
        private ByteSizeValue maxWriteBufferSize;
        private TimeValue maxRetryDelay;
        private TimeValue pollTimeout;

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("[" + NAME_FIELD.getPreferredName() + "] is missing", validationException);
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

        public Integer getMaxBatchOperationCount() {
            return maxBatchOperationCount;
        }

        public void setMaxBatchOperationCount(Integer maxBatchOperationCount) {
            this.maxBatchOperationCount = maxBatchOperationCount;
        }

        public Integer getMaxConcurrentReadBatches() {
            return maxConcurrentReadBatches;
        }

        public void setMaxConcurrentReadBatches(Integer maxConcurrentReadBatches) {
            this.maxConcurrentReadBatches = maxConcurrentReadBatches;
        }

        public ByteSizeValue getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(ByteSizeValue maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
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

        public TimeValue getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(TimeValue pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            name = in.readString();
            remoteCluster = in.readString();
            leaderIndexPatterns = in.readList(StreamInput::readString);
            followIndexNamePattern = in.readOptionalString();
            maxBatchOperationCount = in.readOptionalVInt();
            maxConcurrentReadBatches = in.readOptionalVInt();
            maxBatchSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxConcurrentWriteBatches = in.readOptionalVInt();
            maxWriteBufferCount = in.readOptionalVInt();
            maxWriteBufferSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxRetryDelay = in.readOptionalTimeValue();
            pollTimeout = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(remoteCluster);
            out.writeStringList(leaderIndexPatterns);
            out.writeOptionalString(followIndexNamePattern);
            out.writeOptionalVInt(maxBatchOperationCount);
            out.writeOptionalVInt(maxConcurrentReadBatches);
            out.writeOptionalWriteable(maxBatchSize);
            out.writeOptionalVInt(maxConcurrentWriteBatches);
            out.writeOptionalVInt(maxWriteBufferCount);
            out.writeOptionalWriteable(maxWriteBufferSize);
            out.writeOptionalTimeValue(maxRetryDelay);
            out.writeOptionalTimeValue(pollTimeout);
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
                if (maxBatchOperationCount != null) {
                    builder.field(AutoFollowPattern.MAX_BATCH_OPERATION_COUNT.getPreferredName(), maxBatchOperationCount);
                }
                if (maxBatchSize != null) {
                    builder.field(AutoFollowPattern.MAX_BATCH_SIZE.getPreferredName(), maxBatchSize.getStringRep());
                }
                if (maxWriteBufferCount != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
                }
                if (maxWriteBufferSize != null) {
                    builder.field(AutoFollowPattern.MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
                }
                if (maxConcurrentReadBatches != null) {
                    builder.field(AutoFollowPattern.MAX_CONCURRENT_READ_BATCHES.getPreferredName(), maxConcurrentReadBatches);
                }
                if (maxConcurrentWriteBatches != null) {
                    builder.field(AutoFollowPattern.MAX_CONCURRENT_WRITE_BATCHES.getPreferredName(), maxConcurrentWriteBatches);
                }
                if (maxRetryDelay != null) {
                    builder.field(AutoFollowPattern.MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
                }
                if (pollTimeout != null) {
                    builder.field(AutoFollowPattern.POLL_TIMEOUT.getPreferredName(), pollTimeout.getStringRep());
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
                    Objects.equals(maxBatchOperationCount, request.maxBatchOperationCount) &&
                    Objects.equals(maxConcurrentReadBatches, request.maxConcurrentReadBatches) &&
                    Objects.equals(maxBatchSize, request.maxBatchSize) &&
                    Objects.equals(maxConcurrentWriteBatches, request.maxConcurrentWriteBatches) &&
                    Objects.equals(maxWriteBufferCount, request.maxWriteBufferCount) &&
                    Objects.equals(maxWriteBufferSize, request.maxWriteBufferSize) &&
                    Objects.equals(maxRetryDelay, request.maxRetryDelay) &&
                    Objects.equals(pollTimeout, request.pollTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    name,
                    remoteCluster,
                    leaderIndexPatterns,
                    followIndexNamePattern,
                    maxBatchOperationCount,
                    maxConcurrentReadBatches,
                    maxBatchSize,
                    maxConcurrentWriteBatches,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    pollTimeout);
        }
    }

}
