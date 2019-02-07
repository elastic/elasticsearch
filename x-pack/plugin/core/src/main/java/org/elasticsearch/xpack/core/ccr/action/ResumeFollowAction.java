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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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

    public static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueMinutes(5);

    private ResumeFollowAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {

        static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
        static final ParseField MAX_READ_REQUEST_OPERATION_COUNT = new ParseField("max_read_request_operation_count");
        static final ParseField MAX_READ_REQUEST_SIZE = new ParseField("max_read_request_size");
        static final ParseField MAX_OUTSTANDING_READ_REQUESTS = new ParseField("max_outstanding_read_requests");
        static final ParseField MAX_WRITE_REQUEST_OPERATION_COUNT = new ParseField("max_write_request_operation_count");
        static final ParseField MAX_WRITE_REQUEST_SIZE = new ParseField("max_write_request_size");
        static final ParseField MAX_OUTSTANDING_WRITE_REQUESTS = new ParseField("max_outstanding_write_requests");
        static final ParseField MAX_WRITE_BUFFER_COUNT = new ParseField("max_write_buffer_count");
        static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
        static final ParseField MAX_RETRY_DELAY_FIELD = new ParseField("max_retry_delay");
        static final ParseField READ_POLL_TIMEOUT = new ParseField("read_poll_timeout");
        static final ObjectParser<Request, String> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setFollowerIndex, FOLLOWER_INDEX_FIELD);
            PARSER.declareInt(Request::setMaxReadRequestOperationCount, MAX_READ_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                    Request::setMaxReadRequestSize,
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_READ_REQUEST_SIZE.getPreferredName()), MAX_READ_REQUEST_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareInt(Request::setMaxOutstandingReadRequests, MAX_OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt(Request::setMaxWriteRequestOperationCount, MAX_WRITE_REQUEST_OPERATION_COUNT);
            PARSER.declareField(Request::setMaxWriteRequestSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_REQUEST_SIZE.getPreferredName()), MAX_WRITE_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(Request::setMaxOutstandingWriteRequests, MAX_OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt(Request::setMaxWriteBufferCount, MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                    Request::setMaxWriteBufferSize,
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                    MAX_WRITE_BUFFER_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareField(
                    Request::setMaxRetryDelay,
                    (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY_FIELD.getPreferredName()),
                    MAX_RETRY_DELAY_FIELD,
                    ObjectParser.ValueType.STRING);
            PARSER.declareField(
                    Request::setReadPollTimeout,
                    (p, c) -> TimeValue.parseTimeValue(p.text(), READ_POLL_TIMEOUT.getPreferredName()),
                READ_POLL_TIMEOUT,
                    ObjectParser.ValueType.STRING);
        }

        public static Request fromXContent(final XContentParser parser, final String followerIndex) throws IOException {
            Request request = PARSER.parse(parser, followerIndex);
            if (followerIndex != null) {
                if (request.followerIndex == null) {
                    request.followerIndex = followerIndex;
                } else {
                    if (request.followerIndex.equals(followerIndex) == false) {
                        throw new IllegalArgumentException("provided follower_index is not equal");
                    }
                }
            }
            return request;
        }

        private String followerIndex;

        public String getFollowerIndex() {
            return followerIndex;
        }

        public void setFollowerIndex(String followerIndex) {
            this.followerIndex = followerIndex;
        }

        private Integer maxReadRequestOperationCount;

        public Integer getMaxReadRequestOperationCount() {
            return maxReadRequestOperationCount;
        }

        public void setMaxReadRequestOperationCount(Integer maxReadRequestOperationCount) {
            this.maxReadRequestOperationCount = maxReadRequestOperationCount;
        }

        private Integer maxOutstandingReadRequests;

        public Integer getMaxOutstandingReadRequests() {
            return maxOutstandingReadRequests;
        }

        public void setMaxOutstandingReadRequests(Integer maxOutstandingReadRequests) {
            this.maxOutstandingReadRequests = maxOutstandingReadRequests;
        }

        private ByteSizeValue maxReadRequestSize;

        public ByteSizeValue getMaxReadRequestSize() {
            return maxReadRequestSize;
        }

        public void setMaxReadRequestSize(ByteSizeValue maxReadRequestSize) {
            this.maxReadRequestSize = maxReadRequestSize;
        }

        private Integer maxWriteRequestOperationCount;

        public Integer getMaxWriteRequestOperationCount() {
            return maxWriteRequestOperationCount;
        }

        public void setMaxWriteRequestOperationCount(Integer maxWriteRequestOperationCount) {
            this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
        }

        private ByteSizeValue maxWriteRequestSize;

        public ByteSizeValue getMaxWriteRequestSize() {
            return maxWriteRequestSize;
        }

        public void setMaxWriteRequestSize(ByteSizeValue maxWriteRequestSize) {
            this.maxWriteRequestSize = maxWriteRequestSize;
        }

        private Integer maxOutstandingWriteRequests;

        public Integer getMaxOutstandingWriteRequests() {
            return maxOutstandingWriteRequests;
        }

        public void setMaxOutstandingWriteRequests(Integer maxOutstandingWriteRequests) {
            this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
        }

        private Integer maxWriteBufferCount;

        public Integer getMaxWriteBufferCount() {
            return maxWriteBufferCount;
        }

        public void setMaxWriteBufferCount(Integer maxWriteBufferCount) {
            this.maxWriteBufferCount = maxWriteBufferCount;
        }

        private ByteSizeValue maxWriteBufferSize;

        public ByteSizeValue getMaxWriteBufferSize() {
            return maxWriteBufferSize;
        }

        public void setMaxWriteBufferSize(ByteSizeValue maxWriteBufferSize) {
            this.maxWriteBufferSize = maxWriteBufferSize;
        }

        private TimeValue maxRetryDelay;

        public void setMaxRetryDelay(TimeValue maxRetryDelay) {
            this.maxRetryDelay = maxRetryDelay;
        }

        public TimeValue getMaxRetryDelay() {
            return maxRetryDelay;
        }

        private TimeValue readPollTimeout;

        public TimeValue getReadPollTimeout() {
            return readPollTimeout;
        }

        public void setReadPollTimeout(TimeValue readPollTimeout) {
            this.readPollTimeout = readPollTimeout;
        }

        public Request() {
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;

            if (followerIndex == null) {
                e = addValidationError(FOLLOWER_INDEX_FIELD.getPreferredName() + " is missing", e);
            }
            if (maxReadRequestOperationCount != null && maxReadRequestOperationCount < 1) {
                e = addValidationError(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName() + " must be larger than 0", e);
            }
            if (maxReadRequestSize != null && maxReadRequestSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                e = addValidationError(MAX_READ_REQUEST_SIZE.getPreferredName() + " must be larger than 0", e);
            }
            if (maxOutstandingReadRequests != null && maxOutstandingReadRequests < 1) {
                e = addValidationError(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName() + " must be larger than 0", e);
            }
            if (maxWriteRequestOperationCount != null && maxWriteRequestOperationCount < 1) {
                e = addValidationError(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName() + " must be larger than 0", e);
            }
            if (maxWriteRequestSize != null && maxWriteRequestSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                e = addValidationError(MAX_WRITE_REQUEST_SIZE.getPreferredName() + " must be larger than 0", e);
            }
            if (maxOutstandingWriteRequests != null && maxOutstandingWriteRequests < 1) {
                e = addValidationError(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName() + " must be larger than 0", e);
            }
            if (maxWriteBufferCount != null && maxWriteBufferCount < 1) {
                e = addValidationError(MAX_WRITE_BUFFER_COUNT.getPreferredName() + " must be larger than 0", e);
            }
            if (maxWriteBufferSize != null && maxWriteBufferSize.compareTo(ByteSizeValue.ZERO) <= 0) {
                e = addValidationError(MAX_WRITE_BUFFER_SIZE.getPreferredName() + " must be larger than 0", e);
            }
            if (maxRetryDelay != null && maxRetryDelay.millis() <= 0) {
                String message = "[" + MAX_RETRY_DELAY_FIELD.getPreferredName() + "] must be positive but was [" +
                    maxRetryDelay.getStringRep() + "]";
                e = addValidationError(message, e);
            }
            if (maxRetryDelay != null && maxRetryDelay.millis() > ResumeFollowAction.MAX_RETRY_DELAY.millis()) {
                String message = "[" + MAX_RETRY_DELAY_FIELD.getPreferredName() + "] must be less than [" + MAX_RETRY_DELAY +
                    "] but was [" + maxRetryDelay.getStringRep() + "]";
                e = addValidationError(message, e);
            }

            return e;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            followerIndex = in.readString();
            maxReadRequestOperationCount = in.readOptionalVInt();
            maxOutstandingReadRequests = in.readOptionalVInt();
            maxReadRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxWriteRequestOperationCount = in.readOptionalVInt();
            maxWriteRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxOutstandingWriteRequests = in.readOptionalVInt();
            maxWriteBufferCount = in.readOptionalVInt();
            maxWriteBufferSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxRetryDelay = in.readOptionalTimeValue();
            readPollTimeout = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerIndex);
            out.writeOptionalVInt(maxReadRequestOperationCount);
            out.writeOptionalVInt(maxOutstandingReadRequests);
            out.writeOptionalWriteable(maxReadRequestSize);
            out.writeOptionalVInt(maxWriteRequestOperationCount);
            out.writeOptionalWriteable(maxWriteRequestSize);
            out.writeOptionalVInt(maxOutstandingWriteRequests);
            out.writeOptionalVInt(maxWriteBufferCount);
            out.writeOptionalWriteable(maxWriteBufferSize);
            out.writeOptionalTimeValue(maxRetryDelay);
            out.writeOptionalTimeValue(readPollTimeout);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                toXContentFragment(builder, params);
            }
            builder.endObject();
            return builder;
        }

        void toXContentFragment(final XContentBuilder builder, final Params params) throws IOException {
            builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
            if (maxReadRequestOperationCount != null) {
                builder.field(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
            }
            if (maxReadRequestSize != null) {
                builder.field(MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
            }
            if (maxWriteRequestOperationCount != null) {
                builder.field(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
            }
            if (maxWriteRequestSize != null) {
                builder.field(MAX_WRITE_REQUEST_SIZE.getPreferredName(), maxWriteRequestSize.getStringRep());
            }
            if (maxWriteBufferCount != null) {
                builder.field(MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
            }
            if (maxWriteBufferSize != null) {
                builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
            }
            if (maxOutstandingReadRequests != null) {
                builder.field(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxOutstandingReadRequests);
            }
            if (maxOutstandingWriteRequests != null) {
                builder.field(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxOutstandingWriteRequests);
            }
            if (maxRetryDelay != null) {
                builder.field(MAX_RETRY_DELAY_FIELD.getPreferredName(), maxRetryDelay.getStringRep());
            }
            if (readPollTimeout != null) {
                builder.field(READ_POLL_TIMEOUT.getPreferredName(), readPollTimeout.getStringRep());
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return  Objects.equals(maxReadRequestOperationCount, request.maxReadRequestOperationCount) &&
                    Objects.equals(maxReadRequestSize, request.maxReadRequestSize) &&
                    Objects.equals(maxOutstandingReadRequests, request.maxOutstandingReadRequests) &&
                    Objects.equals(maxWriteRequestOperationCount, request.maxWriteRequestOperationCount) &&
                    Objects.equals(maxWriteRequestSize, request.maxWriteRequestSize) &&
                    Objects.equals(maxOutstandingWriteRequests, request.maxOutstandingWriteRequests) &&
                    Objects.equals(maxWriteBufferCount, request.maxWriteBufferCount) &&
                    Objects.equals(maxWriteBufferSize, request.maxWriteBufferSize) &&
                    Objects.equals(maxRetryDelay, request.maxRetryDelay) &&
                    Objects.equals(readPollTimeout, request.readPollTimeout) &&
                    Objects.equals(followerIndex, request.followerIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    followerIndex,
                    maxReadRequestOperationCount,
                    maxReadRequestSize,
                    maxOutstandingReadRequests,
                    maxWriteRequestOperationCount,
                    maxWriteRequestSize,
                    maxOutstandingWriteRequests,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    readPollTimeout);
        }
    }

}
