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
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

// TODO: This class has become complicated enough that we should implement a xcontent serialization test
public class ReindexTaskStateDoc implements ToXContentObject {

    public static final ConstructingObjectParser<ReindexTaskStateDoc, Void> PARSER =
        new ConstructingObjectParser<>("reindex/index_state", a -> new ReindexTaskStateDoc((ReindexRequest) a[0], (Boolean) a[1],
            (Long) a[2], toTaskId((String) a[3]), (BulkByScrollResponse) a[4],
            (ElasticsearchException) a[5], (Integer) a[6],
            (ScrollableHitSource.Checkpoint) a[7], (BulkByScrollTask.Status) a[8], (float) a[9]));

    private static final String REINDEX_REQUEST = "request";
    private static final String RESILIENT = "resilient";
    private static final String ALLOCATION = "allocation";
    private static final String EPHEMERAL_TASK_ID = "ephemeral_task_id";
    private static final String REINDEX_RESPONSE = "response";
    private static final String REINDEX_EXCEPTION = "exception";
    private static final String FAILURE_REST_STATUS = "failure_rest_status";
    private static final String REINDEX_CHECKPOINT = "checkpoint";
    private static final String REINDEX_CHECKPOINT_STATUS = "checkpoint_status";
    private static final String REQUESTS_PER_SECOND = "requests_per_second";

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReindexRequest.fromXContentWithParams(p),
            new ParseField(REINDEX_REQUEST));
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField(RESILIENT));
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ALLOCATION));
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(EPHEMERAL_TASK_ID));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
            new ParseField(REINDEX_RESPONSE));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(REINDEX_EXCEPTION));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FAILURE_REST_STATUS));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ScrollableHitSource.Checkpoint.fromXContent(p),
            new ParseField(REINDEX_CHECKPOINT));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollTask.Status.fromXContent(p),
            new ParseField(REINDEX_CHECKPOINT_STATUS));
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), new ParseField(REQUESTS_PER_SECOND));
    }

    private final ReindexRequest reindexRequest;
    private final boolean resilient;
    private final Long allocationId;
    private final TaskId ephemeralTaskId;
    private final BulkByScrollResponse reindexResponse;
    private final ElasticsearchException exception;
    private final RestStatus failureStatusCode;
    private final ScrollableHitSource.Checkpoint checkpoint;
    private final BulkByScrollTask.Status checkpointStatus;
    private final float requestsPerSecond;

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, boolean resilient) {
        this(reindexRequest, resilient, null, null, null, null, (RestStatus) null, null, null, reindexRequest.getRequestsPerSecond());
    }

    private ReindexTaskStateDoc(ReindexRequest reindexRequest, boolean resilient, @Nullable Long allocationId,
                                @Nullable TaskId ephemeralTaskId, @Nullable BulkByScrollResponse reindexResponse,
                               @Nullable ElasticsearchException exception,
                                @Nullable Integer failureStatusCode,
                                ScrollableHitSource.Checkpoint checkpoint, @Nullable BulkByScrollTask.Status checkpointStatus,
                                float requestsPerSecond) {
        this(reindexRequest, resilient, allocationId, ephemeralTaskId, reindexResponse, exception,
            failureStatusCode == null ? null : RestStatus.fromCode(failureStatusCode), checkpoint, checkpointStatus, requestsPerSecond);
    }
    private ReindexTaskStateDoc(ReindexRequest reindexRequest, boolean resilient, @Nullable Long allocationId,
                                @Nullable TaskId ephemeralTaskId, @Nullable BulkByScrollResponse reindexResponse,
                               @Nullable ElasticsearchException exception,
                                @Nullable ScrollableHitSource.Checkpoint checkpoint, @Nullable BulkByScrollTask.Status checkpointStatus,
                                float requestsPerSecond) {
        this(reindexRequest, resilient, allocationId, ephemeralTaskId,
            reindexResponse, exception, exception != null ? exception.status() : null,
            checkpoint, checkpointStatus, requestsPerSecond);
    }

    private ReindexTaskStateDoc(ReindexRequest reindexRequest, boolean resilient, @Nullable Long allocationId,
                                @Nullable TaskId ephemeralTaskId,
                                @Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                                @Nullable RestStatus failureStatusCode,
                                @Nullable ScrollableHitSource.Checkpoint checkpoint, @Nullable BulkByScrollTask.Status checkpointStatus,
                                float requestsPerSecond) {
        this.reindexRequest = reindexRequest;
        this.resilient = resilient;
        assert (allocationId == null) == (ephemeralTaskId == null);
        this.allocationId = allocationId;
        this.ephemeralTaskId = ephemeralTaskId;
        assert Float.isNaN(requestsPerSecond) == false && requestsPerSecond >= 0;
        this.requestsPerSecond = requestsPerSecond;
        assert (reindexResponse == null) || (exception == null) : "Either response or exception must be null";
        assert checkpoint != null || checkpointStatus == null : "Can only have checkpointStatus if checkpoint is set";
        this.reindexResponse = reindexResponse;
        this.exception = exception;
        this.failureStatusCode = failureStatusCode;
        this.checkpoint = checkpoint;
        this.checkpointStatus = checkpointStatus;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REINDEX_REQUEST);
        reindexRequest.toXContent(builder, params, true);
        builder.field(RESILIENT, resilient);
        if (allocationId != null) {
            builder.field(ALLOCATION, allocationId);
        }
        if (ephemeralTaskId != null) {
            builder.field(EPHEMERAL_TASK_ID, ephemeralTaskId.toString());
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
            if (checkpointStatus != null) {
                builder.field(REINDEX_CHECKPOINT_STATUS);
                checkpointStatus.toXContent(builder, params);
            }
        }
        builder.field(REQUESTS_PER_SECOND, requestsPerSecond);
        return builder.endObject();
    }

    public static ReindexTaskStateDoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static TaskId toTaskId(String s) {
        return s != null ? new TaskId(s) : null;
    }

    public ReindexRequest getReindexRequest() {
        return reindexRequest;
    }

    public boolean isResilient() {
        return resilient;
    }

    public ReindexRequest getRethrottledReindexRequest() {
        if (reindexRequest.getRequestsPerSecond() != requestsPerSecond) {
            return new ReindexRequest(reindexRequest).setRequestsPerSecond(requestsPerSecond);
        } else {
            return reindexRequest;
        }
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

    public BulkByScrollTask.Status getCheckpointStatus() {
        return checkpointStatus;
    }

    public Long getAllocationId() {
        return allocationId;
    }

    public TaskId getEphemeralTaskId() {
        return ephemeralTaskId;
    }

    public float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public ReindexTaskStateDoc withCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status) {
        // todo: also store and resume from status.
        return new ReindexTaskStateDoc(reindexRequest, resilient, allocationId, ephemeralTaskId,
            reindexResponse, exception, failureStatusCode, checkpoint, status, requestsPerSecond);
    }

    public ReindexTaskStateDoc withNewAllocation(long newAllocationId, TaskId ephemeralTaskId) {
        return new ReindexTaskStateDoc(reindexRequest, resilient, newAllocationId, ephemeralTaskId,
            reindexResponse, exception, failureStatusCode, checkpoint, checkpointStatus, requestsPerSecond);
    }

    public ReindexTaskStateDoc withFinishedState(@Nullable BulkByScrollResponse reindexResponse,
                                                 @Nullable ElasticsearchException exception) {
        return new ReindexTaskStateDoc(reindexRequest, resilient, allocationId, ephemeralTaskId, reindexResponse, exception,
            checkpoint, checkpointStatus, requestsPerSecond);
    }

    public ReindexTaskStateDoc withRequestsPerSecond(float requestsPerSecond) {
        return new ReindexTaskStateDoc(reindexRequest, resilient, allocationId, ephemeralTaskId, reindexResponse, exception,
            checkpoint, checkpointStatus, requestsPerSecond);
    }
}
