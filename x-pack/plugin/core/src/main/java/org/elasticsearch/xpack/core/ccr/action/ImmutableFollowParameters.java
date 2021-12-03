/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ImmutableFollowParameters implements Writeable {

    private final Integer maxReadRequestOperationCount;
    private final Integer maxWriteRequestOperationCount;
    private final Integer maxOutstandingReadRequests;
    private final Integer maxOutstandingWriteRequests;
    private final ByteSizeValue maxReadRequestSize;
    private final ByteSizeValue maxWriteRequestSize;
    private final Integer maxWriteBufferCount;
    private final ByteSizeValue maxWriteBufferSize;
    private final TimeValue maxRetryDelay;
    private final TimeValue readPollTimeout;

    public ImmutableFollowParameters(
        Integer maxReadRequestOperationCount,
        Integer maxWriteRequestOperationCount,
        Integer maxOutstandingReadRequests,
        Integer maxOutstandingWriteRequests,
        ByteSizeValue maxReadRequestSize,
        ByteSizeValue maxWriteRequestSize,
        Integer maxWriteBufferCount,
        ByteSizeValue maxWriteBufferSize,
        TimeValue maxRetryDelay,
        TimeValue readPollTimeout
    ) {
        this.maxReadRequestOperationCount = maxReadRequestOperationCount;
        this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
        this.maxOutstandingReadRequests = maxOutstandingReadRequests;
        this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
        this.maxReadRequestSize = maxReadRequestSize;
        this.maxWriteRequestSize = maxWriteRequestSize;
        this.maxWriteBufferCount = maxWriteBufferCount;
        this.maxWriteBufferSize = maxWriteBufferSize;
        this.maxRetryDelay = maxRetryDelay;
        this.readPollTimeout = readPollTimeout;
    }

    public Integer getMaxReadRequestOperationCount() {
        return maxReadRequestOperationCount;
    }

    public ByteSizeValue getMaxReadRequestSize() {
        return maxReadRequestSize;
    }

    public Integer getMaxOutstandingReadRequests() {
        return maxOutstandingReadRequests;
    }

    public Integer getMaxWriteRequestOperationCount() {
        return maxWriteRequestOperationCount;
    }

    public ByteSizeValue getMaxWriteRequestSize() {
        return maxWriteRequestSize;
    }

    public Integer getMaxOutstandingWriteRequests() {
        return maxOutstandingWriteRequests;
    }

    public Integer getMaxWriteBufferCount() {
        return maxWriteBufferCount;
    }

    public ByteSizeValue getMaxWriteBufferSize() {
        return maxWriteBufferSize;
    }

    public TimeValue getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public TimeValue getReadPollTimeout() {
        return readPollTimeout;
    }

    public ImmutableFollowParameters(StreamInput in) throws IOException {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

    protected XContentBuilder toXContentFragment(final XContentBuilder builder) throws IOException {
        if (maxReadRequestOperationCount != null) {
            builder.field(FollowParameters.MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
        }
        if (maxWriteRequestOperationCount != null) {
            builder.field(FollowParameters.MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
        }
        if (maxOutstandingReadRequests != null) {
            builder.field(FollowParameters.MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxOutstandingReadRequests);
        }
        if (maxOutstandingWriteRequests != null) {
            builder.field(FollowParameters.MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxOutstandingWriteRequests);
        }
        if (maxReadRequestSize != null) {
            builder.field(FollowParameters.MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
        }
        if (maxWriteRequestSize != null) {
            builder.field(FollowParameters.MAX_WRITE_REQUEST_SIZE.getPreferredName(), maxWriteRequestSize.getStringRep());
        }
        if (maxWriteBufferCount != null) {
            builder.field(FollowParameters.MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
        }
        if (maxWriteBufferSize != null) {
            builder.field(FollowParameters.MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
        }
        if (maxRetryDelay != null) {
            builder.field(FollowParameters.MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay.getStringRep());
        }
        if (readPollTimeout != null) {
            builder.field(FollowParameters.READ_POLL_TIMEOUT.getPreferredName(), readPollTimeout.getStringRep());
        }
        return builder;
    }

    public static <P extends ImmutableFollowParameters> void initParser(ConstructingObjectParser<P, ?> parser) {
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), FollowParameters.MAX_READ_REQUEST_OPERATION_COUNT);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), FollowParameters.MAX_WRITE_REQUEST_OPERATION_COUNT);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), FollowParameters.MAX_OUTSTANDING_READ_REQUESTS);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), FollowParameters.MAX_OUTSTANDING_WRITE_REQUESTS);
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowParameters.MAX_READ_REQUEST_SIZE.getPreferredName()),
            FollowParameters.MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowParameters.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            FollowParameters.MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), FollowParameters.MAX_WRITE_BUFFER_COUNT);
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowParameters.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            FollowParameters.MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowParameters.MAX_RETRY_DELAY.getPreferredName()),
            FollowParameters.MAX_RETRY_DELAY,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowParameters.READ_POLL_TIMEOUT.getPreferredName()),
            FollowParameters.READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ImmutableFollowParameters == false) return false;
        ImmutableFollowParameters that = (ImmutableFollowParameters) o;
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
