/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public final class FollowIndexAction extends Action<AcknowledgedResponse> {

    public static final FollowIndexAction INSTANCE = new FollowIndexAction();
    public static final String NAME = "cluster:admin/xpack/ccr/follow_index";

    public static final int DEFAULT_MAX_WRITE_BUFFER_SIZE = 10240;
    public static final int DEFAULT_MAX_BATCH_OPERATION_COUNT = 1024;
    public static final int DEFAULT_MAX_CONCURRENT_READ_BATCHES = 1;
    public static final int DEFAULT_MAX_CONCURRENT_WRITE_BATCHES = 1;
    public static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = Long.MAX_VALUE;
    public static final int RETRY_LIMIT = 10;
    public static final TimeValue DEFAULT_RETRY_TIMEOUT = new TimeValue(500);
    public static final TimeValue DEFAULT_IDLE_SHARD_RETRY_DELAY = TimeValue.timeValueSeconds(10);

    private FollowIndexAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
        private static final ParseField FOLLOWER_INDEX_FIELD = new ParseField("follower_index");
        private static final ParseField MAX_BATCH_OPERATION_COUNT = new ParseField("max_batch_operation_count");
        private static final ParseField MAX_CONCURRENT_READ_BATCHES = new ParseField("max_concurrent_read_batches");
        private static final ParseField MAX_BATCH_SIZE_IN_BYTES = new ParseField("max_batch_size_in_bytes");
        private static final ParseField MAX_CONCURRENT_WRITE_BATCHES = new ParseField("max_concurrent_write_batches");
        private static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
        private static final ParseField RETRY_TIMEOUT = new ParseField("retry_timeout");
        private static final ParseField IDLE_SHARD_RETRY_DELAY = new ParseField("idle_shard_retry_delay");
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(NAME, true,
            (args, followerIndex) -> {
                if (args[1] != null) {
                    followerIndex = (String) args[1];
                }
                return new Request((String) args[0], followerIndex, (Integer) args[2], (Integer) args[3], (Long) args[4],
                    (Integer) args[5], (Integer) args[6], (TimeValue) args[7], (TimeValue) args[8]);
        });

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEADER_INDEX_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FOLLOWER_INDEX_FIELD);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_BATCH_OPERATION_COUNT);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_CONCURRENT_READ_BATCHES);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MAX_BATCH_SIZE_IN_BYTES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_CONCURRENT_WRITE_BATCHES);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_WRITE_BUFFER_SIZE);
            PARSER.declareField(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> TimeValue.parseTimeValue(p.text(), RETRY_TIMEOUT.getPreferredName()),
                    RETRY_TIMEOUT,
                    ObjectParser.ValueType.STRING);
            PARSER.declareField(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> TimeValue.parseTimeValue(p.text(), IDLE_SHARD_RETRY_DELAY.getPreferredName()),
                    IDLE_SHARD_RETRY_DELAY,
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

        private String leaderIndex;

        public String getLeaderIndex() {
            return leaderIndex;
        }


        private String followerIndex;

        public String getFollowerIndex() {
            return followerIndex;
        }

        private int maxBatchOperationCount;

        public int getMaxBatchOperationCount() {
            return maxBatchOperationCount;
        }

        private int maxConcurrentReadBatches;

        public int getMaxConcurrentReadBatches() {
            return maxConcurrentReadBatches;
        }

        private long maxOperationSizeInBytes;

        public long getMaxOperationSizeInBytes() {
            return maxOperationSizeInBytes;
        }

        private int maxConcurrentWriteBatches;

        public int getMaxConcurrentWriteBatches() {
            return maxConcurrentWriteBatches;
        }

        private int maxWriteBufferSize;

        public int getMaxWriteBufferSize() {
            return maxWriteBufferSize;
        }

        private TimeValue retryTimeout;

        public TimeValue getRetryTimeout() {
            return retryTimeout;
        }

        private TimeValue idleShardRetryDelay;

        public TimeValue getIdleShardRetryDelay() {
            return idleShardRetryDelay;
        }

        public Request(
            final String leaderIndex,
            final String followerIndex,
            final Integer maxBatchOperationCount,
            final Integer maxConcurrentReadBatches,
            final Long maxOperationSizeInBytes,
            final Integer maxConcurrentWriteBatches,
            final Integer maxWriteBufferSize,
            final TimeValue retryTimeout,
            final TimeValue idleShardRetryDelay) {

            if (leaderIndex == null) {
                throw new IllegalArgumentException(LEADER_INDEX_FIELD.getPreferredName() + " is missing");
            }

            if (followerIndex == null) {
                throw new IllegalArgumentException(FOLLOWER_INDEX_FIELD.getPreferredName() + " is missing");
            }

            final int actualMaxBatchOperationCount =
                    maxBatchOperationCount == null ? DEFAULT_MAX_BATCH_OPERATION_COUNT : maxBatchOperationCount;
            if (actualMaxBatchOperationCount < 1) {
                throw new IllegalArgumentException(MAX_BATCH_OPERATION_COUNT.getPreferredName() + " must be larger than 0");
            }

            final int actualMaxConcurrentReadBatches =
                    maxConcurrentReadBatches == null ? DEFAULT_MAX_CONCURRENT_READ_BATCHES : maxConcurrentReadBatches;
            if (actualMaxConcurrentReadBatches < 1) {
                throw new IllegalArgumentException(MAX_CONCURRENT_READ_BATCHES.getPreferredName() + " must be larger than 0");
            }

            final long actualMaxOperationSizeInBytes =
                    maxOperationSizeInBytes == null ? DEFAULT_MAX_BATCH_SIZE_IN_BYTES : maxOperationSizeInBytes;
            if (actualMaxOperationSizeInBytes <= 0) {
                throw new IllegalArgumentException(MAX_BATCH_SIZE_IN_BYTES.getPreferredName() + " must be larger than 0");
            }

            final int actualMaxConcurrentWriteBatches =
                    maxConcurrentWriteBatches == null ? DEFAULT_MAX_CONCURRENT_WRITE_BATCHES : maxConcurrentWriteBatches;
            if (actualMaxConcurrentWriteBatches < 1) {
                throw new IllegalArgumentException(MAX_CONCURRENT_WRITE_BATCHES.getPreferredName() + " must be larger than 0");
            }

            final int actualMaxWriteBufferSize = maxWriteBufferSize == null ? DEFAULT_MAX_WRITE_BUFFER_SIZE : maxWriteBufferSize;
            if (actualMaxWriteBufferSize < 1) {
                throw new IllegalArgumentException(MAX_WRITE_BUFFER_SIZE.getPreferredName() + " must be larger than 0");
            }

            final TimeValue actualRetryTimeout = retryTimeout == null ? DEFAULT_RETRY_TIMEOUT : retryTimeout;
            final TimeValue actualIdleShardRetryDelay = idleShardRetryDelay == null ? DEFAULT_IDLE_SHARD_RETRY_DELAY : idleShardRetryDelay;

            this.leaderIndex = leaderIndex;
            this.followerIndex = followerIndex;
            this.maxBatchOperationCount = actualMaxBatchOperationCount;
            this.maxConcurrentReadBatches = actualMaxConcurrentReadBatches;
            this.maxOperationSizeInBytes = actualMaxOperationSizeInBytes;
            this.maxConcurrentWriteBatches = actualMaxConcurrentWriteBatches;
            this.maxWriteBufferSize = actualMaxWriteBufferSize;
            this.retryTimeout = actualRetryTimeout;
            this.idleShardRetryDelay = actualIdleShardRetryDelay;
        }

        public Request() {

        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            leaderIndex = in.readString();
            followerIndex = in.readString();
            maxBatchOperationCount = in.readVInt();
            maxConcurrentReadBatches = in.readVInt();
            maxOperationSizeInBytes = in.readVLong();
            maxConcurrentWriteBatches = in.readVInt();
            maxWriteBufferSize = in.readVInt();
            retryTimeout = in.readOptionalTimeValue();
            idleShardRetryDelay = in.readOptionalTimeValue();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderIndex);
            out.writeString(followerIndex);
            out.writeVInt(maxBatchOperationCount);
            out.writeVInt(maxConcurrentReadBatches);
            out.writeVLong(maxOperationSizeInBytes);
            out.writeVInt(maxConcurrentWriteBatches);
            out.writeVInt(maxWriteBufferSize);
            out.writeOptionalTimeValue(retryTimeout);
            out.writeOptionalTimeValue(idleShardRetryDelay);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
                builder.field(FOLLOWER_INDEX_FIELD.getPreferredName(), followerIndex);
                builder.field(MAX_BATCH_OPERATION_COUNT.getPreferredName(), maxBatchOperationCount);
                builder.field(MAX_BATCH_SIZE_IN_BYTES.getPreferredName(), maxOperationSizeInBytes);
                builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize);
                builder.field(MAX_CONCURRENT_READ_BATCHES.getPreferredName(), maxConcurrentReadBatches);
                builder.field(MAX_CONCURRENT_WRITE_BATCHES.getPreferredName(), maxConcurrentWriteBatches);
                builder.field(RETRY_TIMEOUT.getPreferredName(), retryTimeout.getStringRep());
                builder.field(IDLE_SHARD_RETRY_DELAY.getPreferredName(), idleShardRetryDelay.getStringRep());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return maxBatchOperationCount == request.maxBatchOperationCount &&
                maxConcurrentReadBatches == request.maxConcurrentReadBatches &&
                maxOperationSizeInBytes == request.maxOperationSizeInBytes &&
                maxConcurrentWriteBatches == request.maxConcurrentWriteBatches &&
                maxWriteBufferSize == request.maxWriteBufferSize &&
                Objects.equals(retryTimeout, request.retryTimeout) &&
                Objects.equals(idleShardRetryDelay, request.idleShardRetryDelay) &&
                Objects.equals(leaderIndex, request.leaderIndex) &&
                Objects.equals(followerIndex, request.followerIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                leaderIndex,
                followerIndex,
                maxBatchOperationCount,
                maxConcurrentReadBatches,
                maxOperationSizeInBytes,
                maxConcurrentWriteBatches,
                maxWriteBufferSize,
                retryTimeout,
                idleShardRetryDelay
            );
        }
    }

}
