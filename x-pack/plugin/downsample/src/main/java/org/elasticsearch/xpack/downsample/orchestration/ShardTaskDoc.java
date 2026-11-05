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
 * The state of the downsampling of one source shard. The master creates and (re)assigns it; the assigned node reports on it.
 *
 * @param targetIndex     the name of the downsampled index
 * @param projectId       the project of the source and target indices, and of the state index that holds this document
 * @param targetIndexUuid the uuid of the downsampled index, so a reused index name never adopts this task
 * @param params          what to downsample; its source {@link ShardId} carries the source index uuid
 * @param status          see {@link ShardTaskStatus}
 * @param assignedNode    the id of the node that owns the task, {@code null} when unassigned
 * @param epoch           fencing token, incremented by the master every time it takes the task away from a node (assign,
 *                        unassign, fail), so a node holding an older epoch can no longer write
 * @param attempt         how many times the task was retried
 * @param lastError       the last failure, if any
 * @param retriable       whether the last failure can be retried
 * @param runId           identifies one local run on the assigned node, it changes when the node restarts the run
 * @param progress        the counters of the latest run, as in the task status API; opaque, never read back by the code
 */
public record ShardTaskDoc(
    ProjectId projectId,
    String targetIndex,
    String targetIndexUuid,
    DownsampleShardTaskParams params,
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
            ShardTaskStatus.valueOf((String) a[6]),
            (String) a[7],
            (Long) a[8],
            (Integer) a[9],
            (String) a[10],
            a[11] != null && (Boolean) a[11],
            (String) a[12],
            (Map<String, Object>) a[13]
        )
    );

    static {
        PARSER.declareString(constructorArg(), IndexTaskDoc.PROJECT_ID);
        PARSER.declareString(constructorArg(), IndexTaskDoc.TARGET_INDEX);
        PARSER.declareString(constructorArg(), IndexTaskDoc.TARGET_INDEX_UUID);
        PARSER.declareString(constructorArg(), IndexTaskDoc.SOURCE_INDEX_UUID);
        PARSER.declareInt(constructorArg(), SHARD);
        PARSER.declareObject(constructorArg(), (p, c) -> DownsampleShardTaskParams.fromXContent(p), PARAMS);
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
     * A new, unassigned task for one shard of the source index.
     */
    public static ShardTaskDoc newTask(ProjectId projectId, String targetIndex, String targetIndexUuid, DownsampleShardTaskParams params) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
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
     * Assigns (or reassigns) the task to a node. A retry also counts an attempt.
     */
    public ShardTaskDoc assignTo(String nodeId, boolean isRetry) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            ShardTaskStatus.ASSIGNED,
            Objects.requireNonNull(nodeId),
            epoch + 1,
            isRetry ? attempt + 1 : attempt,
            lastError,
            false,
            null,
            null
        );
    }

    /**
     * Takes the task away from its node without giving it to another one, e.g. when no node holds the source shard.
     */
    public ShardTaskDoc unassign() {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            ShardTaskStatus.UNASSIGNED,
            null,
            epoch + 1,
            attempt,
            lastError,
            false,
            null,
            null
        );
    }

    /**
     * Fails the task for good, e.g. when the retries are exhausted or the target index is gone.
     */
    public ShardTaskDoc failedByMaster(String error) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            ShardTaskStatus.FAILED,
            assignedNode,
            epoch + 1,
            attempt,
            error,
            false,
            runId,
            progress
        );
    }

    // ---- node transitions, they never change the owner, the epoch or the attempt ----

    public ShardTaskDoc running(String newRunId, Map<String, Object> newProgress) {
        return withNodeState(ShardTaskStatus.RUNNING, Objects.requireNonNull(newRunId), newProgress, lastError, retriable);
    }

    public ShardTaskDoc withProgress(Map<String, Object> newProgress) {
        return withNodeState(status, runId, newProgress, lastError, retriable);
    }

    public ShardTaskDoc completed(Map<String, Object> finalProgress) {
        return withNodeState(ShardTaskStatus.COMPLETED, runId, finalProgress, lastError, false);
    }

    public ShardTaskDoc failedOnNode(String error, boolean isRetriable, Map<String, Object> finalProgress) {
        return withNodeState(ShardTaskStatus.FAILED, runId, finalProgress, error, isRetriable);
    }

    private ShardTaskDoc withNodeState(
        ShardTaskStatus newStatus,
        String newRunId,
        Map<String, Object> newProgress,
        String error,
        boolean isRetriable
    ) {
        return new ShardTaskDoc(
            projectId,
            targetIndex,
            targetIndexUuid,
            params,
            newStatus,
            assignedNode,
            epoch,
            attempt,
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
