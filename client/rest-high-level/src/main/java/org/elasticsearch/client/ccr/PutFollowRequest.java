/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class PutFollowRequest implements Validatable, ToXContentObject {

    static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
    static final ParseField LEADER_INDEX_FIELD = new ParseField("leader_index");
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

    private final String remoteCluster;
    private final String leaderIndex;
    private final String followerIndex;
    private Integer maxReadRequestOperationCount;
    private Integer maxOutstandingReadRequests;
    private ByteSizeValue maxReadRequestSize;
    private Integer maxWriteRequestOperationCount;
    private ByteSizeValue maxWriteRequestSize;
    private Integer maxOutstandingWriteRequests;
    private Integer maxWriteBufferCount;
    private ByteSizeValue maxWriteBufferSize;
    private TimeValue maxRetryDelay;
    private TimeValue readPollTimeout;

    public PutFollowRequest(String remoteCluster, String leaderIndex, String followerIndex) {
        this.remoteCluster = Objects.requireNonNull(remoteCluster, "remoteCluster");
        this.leaderIndex = Objects.requireNonNull(leaderIndex, "leaderIndex");
        this.followerIndex = Objects.requireNonNull(followerIndex, "followerIndex");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
        builder.field(LEADER_INDEX_FIELD.getPreferredName(), leaderIndex);
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
        builder.endObject();
        return builder;
    }

    public String getRemoteCluster() {
        return remoteCluster;
    }

    public String getLeaderIndex() {
        return leaderIndex;
    }

    public String getFollowerIndex() {
        return followerIndex;
    }

    public Integer getMaxReadRequestOperationCount() {
        return maxReadRequestOperationCount;
    }

    public void setMaxReadRequestOperationCount(Integer maxReadRequestOperationCount) {
        this.maxReadRequestOperationCount = maxReadRequestOperationCount;
    }

    public Integer getMaxOutstandingReadRequests() {
        return maxOutstandingReadRequests;
    }

    public void setMaxOutstandingReadRequests(Integer maxOutstandingReadRequests) {
        this.maxOutstandingReadRequests = maxOutstandingReadRequests;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutFollowRequest that = (PutFollowRequest) o;
        return Objects.equals(remoteCluster, that.remoteCluster) &&
            Objects.equals(leaderIndex, that.leaderIndex) &&
            Objects.equals(followerIndex, that.followerIndex) &&
            Objects.equals(maxReadRequestOperationCount, that.maxReadRequestOperationCount) &&
            Objects.equals(maxOutstandingReadRequests, that.maxOutstandingReadRequests) &&
            Objects.equals(maxReadRequestSize, that.maxReadRequestSize) &&
            Objects.equals(maxWriteRequestOperationCount, that.maxWriteRequestOperationCount) &&
            Objects.equals(maxWriteRequestSize, that.maxWriteRequestSize) &&
            Objects.equals(maxOutstandingWriteRequests, that.maxOutstandingWriteRequests) &&
            Objects.equals(maxWriteBufferCount, that.maxWriteBufferCount) &&
            Objects.equals(maxWriteBufferSize, that.maxWriteBufferSize) &&
            Objects.equals(maxRetryDelay, that.maxRetryDelay) &&
            Objects.equals(readPollTimeout, that.readPollTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            remoteCluster,
            leaderIndex,
            followerIndex,
            maxReadRequestOperationCount,
            maxOutstandingReadRequests,
            maxReadRequestSize,
            maxWriteRequestOperationCount,
            maxWriteRequestSize,
            maxOutstandingWriteRequests,
            maxWriteBufferCount,
            maxWriteBufferSize,
            maxRetryDelay,
            readPollTimeout
        );
    }
}
