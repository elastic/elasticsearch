/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FollowParameters implements Writeable, ToXContentObject {

    private static final TimeValue RETRY_DELAY_MAX = TimeValue.timeValueMinutes(5);

    public static final ParseField MAX_READ_REQUEST_OPERATION_COUNT = new ParseField("max_read_request_operation_count");
    public static final ParseField MAX_WRITE_REQUEST_OPERATION_COUNT = new ParseField("max_write_request_operation_count");
    public static final ParseField MAX_OUTSTANDING_READ_REQUESTS = new ParseField("max_outstanding_read_requests");
    public static final ParseField MAX_OUTSTANDING_WRITE_REQUESTS = new ParseField("max_outstanding_write_requests");
    public static final ParseField MAX_READ_REQUEST_SIZE = new ParseField("max_read_request_size");
    public static final ParseField MAX_WRITE_REQUEST_SIZE = new ParseField("max_write_request_size");
    public static final ParseField MAX_WRITE_BUFFER_COUNT = new ParseField("max_write_buffer_count");
    public static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
    public static final ParseField MAX_RETRY_DELAY = new ParseField("max_retry_delay");
    public static final ParseField READ_POLL_TIMEOUT = new ParseField("read_poll_timeout");

    Integer maxReadRequestOperationCount;
    Integer maxWriteRequestOperationCount;
    Integer maxOutstandingReadRequests;
    Integer maxOutstandingWriteRequests;
    ByteSizeValue maxReadRequestSize;
    ByteSizeValue maxWriteRequestSize;
    Integer maxWriteBufferCount;
    ByteSizeValue maxWriteBufferSize;
    TimeValue maxRetryDelay;
    TimeValue readPollTimeout;

    public FollowParameters() {}

    public FollowParameters(FollowParameters source) {
        this.maxReadRequestOperationCount = source.maxReadRequestOperationCount;
        this.maxWriteRequestOperationCount = source.maxWriteRequestOperationCount;
        this.maxOutstandingReadRequests = source.maxOutstandingReadRequests;
        this.maxOutstandingWriteRequests = source.maxOutstandingWriteRequests;
        this.maxReadRequestSize = source.maxReadRequestSize;
        this.maxWriteRequestSize = source.maxWriteRequestSize;
        this.maxWriteBufferCount = source.maxWriteBufferCount;
        this.maxWriteBufferSize = source.maxWriteBufferSize;
        this.maxRetryDelay = source.maxRetryDelay;
        this.readPollTimeout = source.readPollTimeout;
    }

    public Integer getMaxReadRequestOperationCount() {
        return maxReadRequestOperationCount;
    }

    public void setMaxReadRequestOperationCount(Integer maxReadRequestOperationCount) {
        this.maxReadRequestOperationCount = maxReadRequestOperationCount;
    }

    public ByteSizeValue getMaxReadRequestSize() {
        return maxReadRequestSize;
    }

    public void setMaxReadRequestSize(ByteSizeValue maxReadRequestSize) {
        this.maxReadRequestSize = maxReadRequestSize;
    }

    public Integer getMaxOutstandingReadRequests() {
        return maxOutstandingReadRequests;
    }

    public void setMaxOutstandingReadRequests(Integer maxOutstandingReadRequests) {
        this.maxOutstandingReadRequests = maxOutstandingReadRequests;
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

    public Integer getMaxOutstandingWriteRequests() {
        return maxOutstandingWriteRequests;
    }

    public void setMaxOutstandingWriteRequests(Integer maxOutstandingWriteRequests) {
        this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
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

    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;

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
            String message = "[" + MAX_RETRY_DELAY.getPreferredName() + "] must be positive but was [" + maxRetryDelay.getStringRep() + "]";
            e = addValidationError(message, e);
        }
        if (maxRetryDelay != null && maxRetryDelay.millis() > RETRY_DELAY_MAX.millis()) {
            String message = "["
                + MAX_RETRY_DELAY.getPreferredName()
                + "] must be less than ["
                + RETRY_DELAY_MAX.getStringRep()
                + "] but was ["
                + maxRetryDelay.getStringRep()
                + "]";
            e = addValidationError(message, e);
        }

        return e;
    }

    public FollowParameters(StreamInput in) throws IOException {
        fromStreamInput(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

    void fromStreamInput(StreamInput in) throws IOException {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder);
        builder.endObject();
        return builder;
    }

    XContentBuilder toXContentFragment(final XContentBuilder builder) throws IOException {
        if (maxReadRequestOperationCount != null) {
            builder.field(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
        }
        if (maxWriteRequestOperationCount != null) {
            builder.field(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
        }
        if (maxOutstandingReadRequests != null) {
            builder.field(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxOutstandingReadRequests);
        }
        if (maxOutstandingWriteRequests != null) {
            builder.field(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxOutstandingWriteRequests);
        }
        if (maxReadRequestSize != null) {
            builder.field(MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
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
        if (maxRetryDelay != null) {
            builder.field(MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
        }
        if (readPollTimeout != null) {
            builder.field(READ_POLL_TIMEOUT.getPreferredName(), readPollTimeout.getStringRep());
        }
        return builder;
    }

    public static <P extends FollowParameters> void initParser(AbstractObjectParser<P, ?> parser) {
        parser.declareInt(FollowParameters::setMaxReadRequestOperationCount, MAX_READ_REQUEST_OPERATION_COUNT);
        parser.declareInt(FollowParameters::setMaxWriteRequestOperationCount, MAX_WRITE_REQUEST_OPERATION_COUNT);
        parser.declareInt(FollowParameters::setMaxOutstandingReadRequests, MAX_OUTSTANDING_READ_REQUESTS);
        parser.declareInt(FollowParameters::setMaxOutstandingWriteRequests, MAX_OUTSTANDING_WRITE_REQUESTS);
        parser.declareField(
            FollowParameters::setMaxReadRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_READ_REQUEST_SIZE.getPreferredName()),
            MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            FollowParameters::setMaxWriteRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareInt(FollowParameters::setMaxWriteBufferCount, MAX_WRITE_BUFFER_COUNT);
        parser.declareField(
            FollowParameters::setMaxWriteBufferSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            FollowParameters::setMaxRetryDelay,
            (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY.getPreferredName()),
            MAX_RETRY_DELAY,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            FollowParameters::setReadPollTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), READ_POLL_TIMEOUT.getPreferredName()),
            READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof FollowParameters == false) return false;
        FollowParameters that = (FollowParameters) o;
        return Objects.equals(maxReadRequestOperationCount, that.maxReadRequestOperationCount)
            && Objects.equals(maxWriteRequestOperationCount, that.maxWriteRequestOperationCount)
            && Objects.equals(maxOutstandingReadRequests, that.maxOutstandingReadRequests)
            && Objects.equals(maxOutstandingWriteRequests, that.maxOutstandingWriteRequests)
            && Objects.equals(maxReadRequestSize, that.maxReadRequestSize)
            && Objects.equals(maxWriteRequestSize, that.maxWriteRequestSize)
            && Objects.equals(maxWriteBufferCount, that.maxWriteBufferCount)
            && Objects.equals(maxWriteBufferSize, that.maxWriteBufferSize)
            && Objects.equals(maxRetryDelay, that.maxRetryDelay)
            && Objects.equals(readPollTimeout, that.readPollTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            maxReadRequestOperationCount,
            maxWriteRequestOperationCount,
            maxOutstandingReadRequests,
            maxOutstandingWriteRequests,
            maxReadRequestSize,
            maxWriteRequestSize,
            maxWriteBufferCount,
            maxWriteBufferSize,
            maxRetryDelay,
            readPollTimeout
        );
    }
}
