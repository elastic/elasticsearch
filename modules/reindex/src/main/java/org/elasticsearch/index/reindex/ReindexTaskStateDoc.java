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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class ReindexTaskStateDoc implements ToXContentObject {

    public static final ConstructingObjectParser<ReindexTaskStateDoc, Void> PARSER =
        new ConstructingObjectParser<>("reindex/index_state", a -> new ReindexTaskStateDoc((ReindexRequest) a[0], (Long) a[1], (Long) a[2],
            (Long) a[3], (BulkByScrollResponse) a[4], (ElasticsearchException) a[5], (Integer) a[6],
            (ScrollableHitSource.Checkpoint) a[7]));

    private static final String STATE_TIME_MILLIS = "start_time_epoch_millis";
    private static final String END_TIME_MILLIS = "end_time_epoch_millis";
    private static final String REINDEX_REQUEST = "request";
    private static final String ALLOCATION = "allocation";
    private static final String REINDEX_RESPONSE = "response";
    private static final String REINDEX_EXCEPTION = "exception";
    private static final String FAILURE_REST_STATUS = "failure_rest_status";
    private static final String REINDEX_CHECKPOINT = "checkpoint";

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReindexRequest.fromXContentWithParams(p),
            new ParseField(REINDEX_REQUEST));
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField(STATE_TIME_MILLIS));
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ALLOCATION));
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(END_TIME_MILLIS));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
            new ParseField(REINDEX_RESPONSE));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(REINDEX_EXCEPTION));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FAILURE_REST_STATUS));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ScrollableHitSource.Checkpoint.fromXContent(p),
            new ParseField(REINDEX_CHECKPOINT));
    }

    private final long startTimeMillis;
    private final ReindexRequest reindexRequest;
    private final Long allocationId;
    private final Long endTimeMillis;
    private final BulkByScrollResponse reindexResponse;
    private final ElasticsearchException exception;
    private final RestStatus failureStatusCode;
    private final ScrollableHitSource.Checkpoint checkpoint;

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, long startTimeMillis) {
        this(reindexRequest, startTimeMillis, null, null, null, null, (RestStatus) null, null);
    }

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, long startTimeMillis, @Nullable Long allocationId,
                               @Nullable Long endTimeMillis, @Nullable BulkByScrollResponse reindexResponse,
                               @Nullable ElasticsearchException exception, @Nullable Integer failureStatusCode,
                               ScrollableHitSource.Checkpoint checkpoint) {
        this(reindexRequest, startTimeMillis, allocationId, endTimeMillis, reindexResponse, exception,
            failureStatusCode == null ? null : RestStatus.fromCode(failureStatusCode), checkpoint);
    }

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, long startTimeMillis, @Nullable Long allocationId,
                               @Nullable Long endTimeMillis, @Nullable BulkByScrollResponse reindexResponse,
                               @Nullable ElasticsearchException exception, @Nullable ScrollableHitSource.Checkpoint checkpoint) {
        this(reindexRequest, startTimeMillis, allocationId, endTimeMillis, reindexResponse, exception,
            exception != null ? exception.status() : null, checkpoint);
    }

    private ReindexTaskStateDoc(ReindexRequest reindexRequest, long startTimeMillis, @Nullable Long allocationId,
                                @Nullable Long endTimeMillis, @Nullable BulkByScrollResponse reindexResponse,
                                @Nullable ElasticsearchException exception, @Nullable RestStatus failureStatusCode,
                                @Nullable ScrollableHitSource.Checkpoint checkpoint) {
        this.startTimeMillis = startTimeMillis;
        this.allocationId = allocationId;
        this.endTimeMillis = endTimeMillis;
        assert (reindexResponse == null) || (exception == null) : "Either response or exception must be null";
        this.reindexRequest = reindexRequest;
        this.reindexResponse = reindexResponse;
        this.exception = exception;
        this.failureStatusCode = failureStatusCode;
        this.checkpoint = checkpoint;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REINDEX_REQUEST);
        reindexRequest.toXContent(builder, params, true);
        builder.field(STATE_TIME_MILLIS, startTimeMillis);
        if (allocationId != null) {
            builder.field(ALLOCATION, allocationId);
        }
        if (endTimeMillis != null) {
            builder.field(END_TIME_MILLIS, endTimeMillis);
        }
        if (reindexResponse != null) {
            builder.field(REINDEX_RESPONSE);
            builder.startObject();
            reindexResponse.toXContent(builder, params);
            builder.endObject();
        }
        if (exception != null) {
            builder.field(REINDEX_EXCEPTION);
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, exception);
            builder.endObject();
            builder.field(FAILURE_REST_STATUS, failureStatusCode.getStatus());
        }
        if (checkpoint != null) {
            builder.field(REINDEX_CHECKPOINT);
            checkpoint.toXContent(builder, params);
        }
        return builder.endObject();
    }

    public static ReindexTaskStateDoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReindexRequest getReindexRequest() {
        return reindexRequest;
    }

    public BulkByScrollResponse getReindexResponse() {
        return reindexResponse;
    }

    public ElasticsearchException getException() {
        return exception;
    }

    public RestStatus getFailureStatusCode() {
        return failureStatusCode;
    }

    public ScrollableHitSource.Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public Long getAllocationId() {
        return allocationId;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public Long getEndTimeMillis() {
        return endTimeMillis;
    }

    public ReindexTaskStateDoc withCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status) {
        // todo: also store and resume from status.
        return new ReindexTaskStateDoc(reindexRequest, startTimeMillis, allocationId, endTimeMillis, reindexResponse, exception,
            failureStatusCode, checkpoint);
    }

    public ReindexTaskStateDoc withNewAllocation(long newAllocationId) {
        return new ReindexTaskStateDoc(reindexRequest, startTimeMillis, newAllocationId, endTimeMillis, reindexResponse, exception,
            failureStatusCode, checkpoint);
    }

    public ReindexTaskStateDoc withFinishedState(long endTimeMillis, @Nullable BulkByScrollResponse reindexResponse,
                                                 @Nullable ElasticsearchException exception) {
        return new ReindexTaskStateDoc(reindexRequest, startTimeMillis, allocationId, endTimeMillis, reindexResponse, exception,
            checkpoint);
    }
}
