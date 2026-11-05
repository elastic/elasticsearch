/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.downsample.DownsampleShardProgress;
import org.elasticsearch.xpack.downsample.DownsampleShardTaskParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The state of the downsampling of one source shard. The master creates it, releases it when its node is gone, retries or fails it;
 * a node with a free slot and a copy of the source shard claims it, runs it and reports on it.
 *
 * @param projectId       the project of the source and target indices, and of the state index that holds this document
 * @param targetIndex     the name of the downsampled index
 * @param targetIndexUuid the uuid of the downsampled index, so a reused index name never adopts this task
 * @param params          what to downsample; its source {@link ShardId} carries the source index uuid
 * @param createdAt       epoch millis when the operation was requested: nodes claim older tasks first
 * @param status          see {@link ShardTaskStatus}
 * @param assignedNode    the id of the node that claimed the task, {@code null} when unassigned
 * @param epoch           fencing token, incremented every time the task changes owner (claimed by a node, released, retried or
 *                        failed by the master), so a node holding an older epoch can no longer write
 * @param attempt         how many times the task was retried
 * @param lastError       the last failure, if any
 * @param retriable       whether the last failure can be retried
 * @param runId           identifies one local run on the owning node, it changes when the node restarts the run
 * @param progress        the counters of the latest run, as in the task status API; opaque, never read back by the code
 */
public record ShardTaskDoc(
    ProjectId projectId,
    String targetIndex,
    String targetIndexUuid,
    DownsampleShardTaskParams params,
    long createdAt,
    ShardTaskStatus status,
    @Nullable String assignedNode,
    long epoch,
    int attempt,
    @Nullable String lastError,
    boolean retriable,
    @Nullable String runId,
    @Nullable Map<String, Object> progress
) implements StateDoc {

    static final ParseField SHARD = new ParseField("shard");
    static final ParseField PARAMS = new ParseField("params");
    static final ParseField STATUS = new ParseField("status");
    static final ParseField ASSIGNED_NODE = new ParseField("assigned_node");
    static final ParseField EPOCH = new ParseField("epoch");
    static final ParseField RETRIABLE = new ParseField("retriable");
    static final ParseField RUN_ID = new ParseField("run_id");
    static final ParseField PROGRESS = new ParseField("progress");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ShardTaskDoc, Void> PARSER = new ConstructingObjectParser<>(
        "downsample_shard_task",
        false,
        a -> new ShardTaskDoc(
            ProjectId.fromId((String) a[0]),
            (String) a[1],
            (String) a[2],
            withSourceIndexUuid((DownsampleShardTaskParams) a[5], (String) a[3], (Integer) a[4]),
            (Long) a[6],
            ShardTaskStatus.valueOf((String) a[7]),
            (String) a[8],
            (Long) a[9],
            (Integer) a[10],
            (String) a[11],
            a[12] != null && (Boolean) a[12],
            (String) a[13],
            (Map<String, Object>) a[14]
        )
    );

    static {
        PARSER.declareString(constructorArg(), IndexTaskDoc.PROJECT_ID);
        PARSER.declareString(constructorArg(), IndexTaskDoc.TARGET_INDEX);
        PARSER.declareString(constructorArg(), IndexTaskDoc.TARGET_INDEX_UUID);
        PARSER.declareString(constructorArg(), IndexTaskDoc.SOURCE_INDEX_UUID);
        PARSER.declareInt(constructorArg(), SHARD);
        PARSER.declareObject(constructorArg(), (p, c) -> DownsampleShardTaskParams.fromXContent(p), PARAMS);
        PARSER.declareLong(constructorArg(), IndexTaskDoc.CREATED_AT);
        PARSER.declareString(constructorArg(), STATUS);
        PARSER.declareString(optionalConstructorArg(), ASSIGNED_NODE);
        PARSER.declareLong(constructorArg(), EPOCH);
        PARSER.declareInt(constructorArg(), IndexTaskDoc.ATTEMPT);
        PARSER.declareString(optionalConstructorArg(), IndexTaskDoc.LAST_ERROR);
        PARSER.declareBoolean(optionalConstructorArg(), RETRIABLE);
        PARSER.declareString(optionalConstructorArg(), RUN_ID);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), PROGRESS);
        PARSER.declareString(
            (doc, type) -> { assert DownsampleStateIndex.SHARD_TASK_DOC_TYPE.equals(type) : "unexpected doc type [" + type + "]"; },
            IndexTaskDoc.DOC_TYPE
        );
    }

    public ShardTaskDoc {
        Objects.requireNonNull(projectId);
        Objects.requireNonNull(targetIndex);
        Objects.requireNonNull(targetIndexUuid);
        Objects.requireNonNull(params);
        Objects.requireNonNull(status);
    }

    /**
     * A new, unassigned task for one shard of the source index, of an operation requested at {@code createdAt}.
     */
    public static ShardTaskDoc newTask(
        ProjectId projectId,
        String targetIndex,
        String targetIndexUuid,
        DownsampleShardTaskParams params,
        long createdAt
    ) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            createdAt,
            ShardTaskStatus.UNASSIGNED,
            null,
            0L,
            0,
            null,
            false,
            null,
            null
        );
    }

    public static ShardTaskDoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String id() {
        return DownsampleStateIndex.shardTaskId(targetIndex, targetIndexUuid, shard());
    }

    public int shard() {
        return params.shardId().id();
    }

    public ShardId sourceShardId() {
        return params.shardId();
    }

    // ---- master transitions, each takes the task away from whichever node held it, so each increments the epoch ----

    /**
     * Releases the task of a node that left the cluster, so another node can claim it. Not an attempt: like a persistent task, a
     * shard task survives any number of node restarts.
     */
    public ShardTaskDoc unassign() {
        return copy(ShardTaskStatus.UNASSIGNED, null, epoch + 1, attempt, lastError, false, null, null);
    }

    /**
     * Makes a task that failed with a retriable error claimable again, counting an attempt.
     */
    public ShardTaskDoc retry() {
        return copy(ShardTaskStatus.UNASSIGNED, null, epoch + 1, attempt + 1, lastError, false, null, null);
    }

    /**
     * Fails the task for good, e.g. when the retries are exhausted.
     */
    public ShardTaskDoc failedByMaster(String error) {
        return copy(ShardTaskStatus.FAILED, assignedNode, epoch + 1, attempt, error, false, runId, progress);
    }

    // ---- node transitions ----

    /**
     * A node claims an unassigned task and starts running it, in one write: it becomes the owner, at the next epoch.
     */
    public ShardTaskDoc claimBy(String nodeId, String newRunId, Map<String, Object> newProgress) {
        return copy(
            ShardTaskStatus.RUNNING,
            Objects.requireNonNull(nodeId),
            epoch + 1,
            attempt,
            lastError,
            false,
            Objects.requireNonNull(newRunId),
            newProgress
        );
    }

    /**
     * The owner starts a new run of a task it already runs (e.g. after it restarted). The owner, epoch and attempt do not change.
     */
    public ShardTaskDoc running(String newRunId, Map<String, Object> newProgress) {
        return copy(
            ShardTaskStatus.RUNNING,
            assignedNode,
            epoch,
            attempt,
            lastError,
            retriable,
            Objects.requireNonNull(newRunId),
            newProgress
        );
    }

    public ShardTaskDoc withProgress(Map<String, Object> newProgress) {
        return copy(status, assignedNode, epoch, attempt, lastError, retriable, runId, newProgress);
    }

    public ShardTaskDoc completed(Map<String, Object> finalProgress) {
        return copy(ShardTaskStatus.COMPLETED, assignedNode, epoch, attempt, lastError, false, runId, finalProgress);
    }

    public ShardTaskDoc failedOnNode(String error, boolean isRetriable, Map<String, Object> finalProgress) {
        return copy(ShardTaskStatus.FAILED, assignedNode, epoch, attempt, error, isRetriable, runId, finalProgress);
    }

    private ShardTaskDoc copy(
        ShardTaskStatus newStatus,
        String newAssignedNode,
        long newEpoch,
        int newAttempt,
        String error,
        boolean isRetriable,
        String newRunId,
        Map<String, Object> newProgress
    ) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            createdAt,
            newStatus,
            newAssignedNode,
            newEpoch,
            newAttempt,
            error,
            isRetriable,
            newRunId,
            newProgress
        );
    }

    /**
     * Converts the counters of a run to the {@code progress} object, using the same format as the task status API.
     */
    public static Map<String, Object> progressOf(DownsampleShardProgress progress) {
        try {
            return XContentHelper.convertToMap(
                XContentHelper.toXContent(progress.toStatus(), XContentType.JSON, ToXContent.EMPTY_PARAMS, false),
                false,
                XContentType.JSON
            ).v2();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@link DownsampleShardTaskParams} writes its shard id as {@code [index][shard]}, which loses the index uuid, so the
     * uuid is stored next to it and put back on parsing.
     */
    private static DownsampleShardTaskParams withSourceIndexUuid(DownsampleShardTaskParams params, String sourceIndexUuid, int shard) {
        ShardId parsed = params.shardId();
        if (parsed.id() != shard) {
            throw new IllegalArgumentException("shard [" + shard + "] does not match the params shard id [" + parsed + "]");
        }
        return new DownsampleShardTaskParams(
            params.downsampleConfig(),
            params.downsampleIndex(),
            params.indexStartTimeMillis(),
            params.indexEndTimeMillis(),
            new ShardId(new Index(parsed.getIndexName(), sourceIndexUuid), shard),
            params.metrics(),
            params.labels(),
            params.dimensions(),
            params.multiFieldSources()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params xContentParams) throws IOException {
        builder.startObject();
        builder.field(IndexTaskDoc.DOC_TYPE.getPreferredName(), DownsampleStateIndex.SHARD_TASK_DOC_TYPE);
        builder.field(IndexTaskDoc.PROJECT_ID.getPreferredName(), projectId.id());
        builder.field(IndexTaskDoc.TARGET_INDEX.getPreferredName(), targetIndex);
        builder.field(IndexTaskDoc.TARGET_INDEX_UUID.getPreferredName(), targetIndexUuid);
        builder.field(IndexTaskDoc.SOURCE_INDEX_UUID.getPreferredName(), params.shardId().getIndex().getUUID());
        builder.field(SHARD.getPreferredName(), shard());
        builder.field(PARAMS.getPreferredName(), params);
        builder.field(IndexTaskDoc.CREATED_AT.getPreferredName(), createdAt);
        builder.field(STATUS.getPreferredName(), status.name());
        if (assignedNode != null) {
            builder.field(ASSIGNED_NODE.getPreferredName(), assignedNode);
        }
        builder.field(EPOCH.getPreferredName(), epoch);
        builder.field(IndexTaskDoc.ATTEMPT.getPreferredName(), attempt);
        if (lastError != null) {
            builder.field(IndexTaskDoc.LAST_ERROR.getPreferredName(), lastError);
        }
        builder.field(RETRIABLE.getPreferredName(), retriable);
        if (runId != null) {
            builder.field(RUN_ID.getPreferredName(), runId);
        }
        if (progress != null) {
            builder.field(PROGRESS.getPreferredName(), progress);
        }
        return builder.endObject();
    }
}
