/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The state of one downsample operation, identified by its project and target index name. Only the master writes it.
 *
 * @param projectId       the project of the source and target indices, and of the state index that holds this document
 * @param targetIndex     the name of the downsampled index
 * @param targetIndexUuid the uuid of the target index, {@code null} until the target index has been created; recorded once
 *                        and never changed, it ties the operation to that exact index even if the name is reused
 * @param sourceIndex     the name of the index being downsampled
 * @param sourceIndexUuid the uuid of the index being downsampled
 * @param config          the requested downsample configuration
 * @param phase           where the master is in the operation
 * @param attempt         how many times the current phase has failed and been retried
 * @param nextRetryAt     epoch millis before which the master does not retry the current phase, {@code 0} when no retry is
 *                        pending
 * @param lastError       the last failure, if any
 * @param errorHistory    the most recent failures, oldest first, capped at {@link #MAX_ERROR_HISTORY}
 * @param createdAt       epoch millis when the operation was requested
 * @param deadline        epoch millis after which the operation is failed instead of retried
 */
public record IndexTaskDoc(
    ProjectId projectId,
    String targetIndex,
    @Nullable String targetIndexUuid,
    String sourceIndex,
    String sourceIndexUuid,
    DownsampleConfig config,
    IndexTaskPhase phase,
    int attempt,
    long nextRetryAt,
    @Nullable String lastError,
    List<String> errorHistory,
    long createdAt,
    long deadline
) implements StateDoc {

    static final int MAX_ERROR_HISTORY = 10;

    static final ParseField DOC_TYPE = new ParseField(DownsampleStateIndex.DOC_TYPE_FIELD);
    static final ParseField PROJECT_ID = new ParseField("project_id");
    static final ParseField TARGET_INDEX = new ParseField("target_index");
    static final ParseField TARGET_INDEX_UUID = new ParseField("target_index_uuid");
    static final ParseField SOURCE_INDEX = new ParseField("source_index");
    static final ParseField SOURCE_INDEX_UUID = new ParseField("source_index_uuid");
    static final ParseField CONFIG = new ParseField("config");
    static final ParseField PHASE = new ParseField("phase");
    static final ParseField ATTEMPT = new ParseField("attempt");
    static final ParseField NEXT_RETRY_AT = new ParseField("next_retry_at");
    static final ParseField LAST_ERROR = new ParseField("last_error");
    static final ParseField ERROR_HISTORY = new ParseField("error_history");
    static final ParseField CREATED_AT = new ParseField("created_at");
    static final ParseField DEADLINE = new ParseField("deadline");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexTaskDoc, Void> PARSER = new ConstructingObjectParser<>(
        "downsample_index_task",
        false,
        a -> new IndexTaskDoc(
            ProjectId.fromId((String) a[0]),
            (String) a[1],
            (String) a[2],
            (String) a[3],
            (String) a[4],
            (DownsampleConfig) a[5],
            IndexTaskPhase.valueOf((String) a[6]),
            (Integer) a[7],
            a[8] == null ? 0L : (Long) a[8],
            (String) a[9],
            a[10] == null ? List.of() : (List<String>) a[10],
            (Long) a[11],
            (Long) a[12]
        )
    );

    static {
        PARSER.declareString(constructorArg(), PROJECT_ID);
        PARSER.declareString(constructorArg(), TARGET_INDEX);
        PARSER.declareString(optionalConstructorArg(), TARGET_INDEX_UUID);
        PARSER.declareString(constructorArg(), SOURCE_INDEX);
        PARSER.declareString(constructorArg(), SOURCE_INDEX_UUID);
        PARSER.declareObject(constructorArg(), (p, c) -> DownsampleConfig.fromXContent(p), CONFIG);
        PARSER.declareString(constructorArg(), PHASE);
        PARSER.declareInt(constructorArg(), ATTEMPT);
        PARSER.declareLong(optionalConstructorArg(), NEXT_RETRY_AT);
        PARSER.declareString(optionalConstructorArg(), LAST_ERROR);
        PARSER.declareStringArray(optionalConstructorArg(), ERROR_HISTORY);
        PARSER.declareLong(constructorArg(), CREATED_AT);
        PARSER.declareLong(constructorArg(), DEADLINE);
        PARSER.declareString(
            (doc, type) -> { assert DownsampleStateIndex.INDEX_TASK_DOC_TYPE.equals(type) : "unexpected doc type [" + type + "]"; },
            DOC_TYPE
        );
    }

    public IndexTaskDoc {
        Objects.requireNonNull(projectId);
        Objects.requireNonNull(targetIndex);
        Objects.requireNonNull(sourceIndex);
        Objects.requireNonNull(sourceIndexUuid);
        Objects.requireNonNull(config);
        Objects.requireNonNull(phase);
        errorHistory = List.copyOf(errorHistory);
    }

    /**
     * A new operation, before the target index exists.
     */
    public static IndexTaskDoc newTask(
        ProjectId projectId,
        String targetIndex,
        String sourceIndex,
        String sourceIndexUuid,
        DownsampleConfig config,
        long now,
        long deadline
    ) {
        return new IndexTaskDoc(
            projectId,
            targetIndex,
            null,
            sourceIndex,
            sourceIndexUuid,
            config,
            IndexTaskPhase.CREATE_TARGET,
            0,
            0L,
            null,
            List.of(),
            now,
            deadline
        );
    }

    public static IndexTaskDoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String id() {
        return DownsampleStateIndex.indexTaskId(targetIndex);
    }

    /**
     * Moves to the next phase, clearing the retry bookkeeping of the previous one.
     */
    public IndexTaskDoc withPhase(IndexTaskPhase nextPhase) {
        return copy(targetIndexUuid, nextPhase, 0, 0L, lastError, errorHistory);
    }

    /**
     * Records the uuid of the newly created target index and moves on to running the shard tasks, in one write.
     */
    public IndexTaskDoc targetCreated(String uuid) {
        return copy(Objects.requireNonNull(uuid), IndexTaskPhase.RUN_SHARDS, 0, 0L, lastError, errorHistory);
    }

    /**
     * Records a retriable failure of the current phase.
     */
    public IndexTaskDoc withRetry(String error, long retryAt) {
        return copy(targetIndexUuid, phase, attempt + 1, retryAt, error, appendError(error));
    }

    /**
     * Fails the whole operation, this is terminal.
     */
    public IndexTaskDoc failed(String error) {
        return copy(targetIndexUuid, IndexTaskPhase.FAILED, attempt, 0L, error, appendError(error));
    }

    private IndexTaskDoc copy(
        String newTargetIndexUuid,
        IndexTaskPhase newPhase,
        int newAttempt,
        long newNextRetryAt,
        String newLastError,
        List<String> newErrorHistory
    ) {
        return new IndexTaskDoc(
            projectId,
            targetIndex,
            newTargetIndexUuid,
            sourceIndex,
            sourceIndexUuid,
            config,
            newPhase,
            newAttempt,
            newNextRetryAt,
            newLastError,
            newErrorHistory,
            createdAt,
            deadline
        );
    }

    private List<String> appendError(String error) {
        List<String> history = new ArrayList<>(errorHistory);
        history.add(error);
        return history.size() <= MAX_ERROR_HISTORY ? history : history.subList(history.size() - MAX_ERROR_HISTORY, history.size());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DOC_TYPE.getPreferredName(), DownsampleStateIndex.INDEX_TASK_DOC_TYPE);
        builder.field(PROJECT_ID.getPreferredName(), projectId.id());
        builder.field(TARGET_INDEX.getPreferredName(), targetIndex);
        if (targetIndexUuid != null) {
            builder.field(TARGET_INDEX_UUID.getPreferredName(), targetIndexUuid);
        }
        builder.field(SOURCE_INDEX.getPreferredName(), sourceIndex);
        builder.field(SOURCE_INDEX_UUID.getPreferredName(), sourceIndexUuid);
        builder.field(CONFIG.getPreferredName(), config);
        builder.field(PHASE.getPreferredName(), phase.name());
        builder.field(ATTEMPT.getPreferredName(), attempt);
        if (nextRetryAt > 0) {
            builder.field(NEXT_RETRY_AT.getPreferredName(), nextRetryAt);
        }
        if (lastError != null) {
            builder.field(LAST_ERROR.getPreferredName(), lastError);
        }
        builder.stringListField(ERROR_HISTORY.getPreferredName(), errorHistory);
        builder.field(CREATED_AT.getPreferredName(), createdAt);
        builder.field(DEADLINE.getPreferredName(), deadline);
        return builder.endObject();
    }
}
