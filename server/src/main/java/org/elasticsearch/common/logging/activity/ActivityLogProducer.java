/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.index.ActionLoggingFields;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.logging.activity.QueryLogging.QUERY_FIELD_SHARDS;

/**
 * Generic log producer class.
 * Each log producer receives a context and decides whether to log, and at which level. Then it extracts logging information
 * from the context and places it into the message. The producer defines which fields are included in the specific log message.
 *
 * @param <Context> Specific logger context
 */
public interface ActivityLogProducer<Context extends ActivityLoggerContext> {

    String X_OPAQUE_ID_FIELD = "http.request.headers.x_opaque_id";
    String EVENT_OUTCOME_FIELD = "event.outcome";
    String EVENT_DURATION_FIELD = "event.duration";
    String TRACE_ID_FIELD = "trace.id";
    String TASK_ID_FIELD = "elasticsearch.task.id";
    String PARENT_TASK_ID_FIELD = "elasticsearch.parent.task.id";
    String PARENT_NODE_ID_FIELD = "elasticsearch.parent.node.id";

    /**
     * Produces a {@link ESLogMessage} if the producer decides to log, or nothing otherwise.
     */
    Optional<ESLogMessage> produce(Context context, ActionLoggingFields additionalFields);

    /**
     * Since we only have query logging for now, set it as the default logger name for convenience.
     */
    default String loggerName() {
        return QueryLogging.QUERY_LOGGER_NAME;
    }

    /**
     * Produces a {@link ESLogMessage} with common fields.
     */
    default ESLogMessage produceCommon(Context context, String prefix, ActionLoggingFields additionalFields) {
        var fields = new ESLogMessage();
        fields.withFields(additionalFields.logFields());
        fields.field(X_OPAQUE_ID_FIELD, context.getOpaqueId());
        fields.field(TRACE_ID_FIELD, context.getTraceId());
        long tookInNanos = context.getTookInNanos();
        fields.field(EVENT_DURATION_FIELD, tookInNanos);
        fields.field(EVENT_OUTCOME_FIELD, context.isSuccess() ? "success" : "failure");
        fields.field(prefix + "took", tookInNanos);
        fields.field(prefix + "took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        fields.field(prefix + "type", context.getType());
        fields.field(TASK_ID_FIELD, context.getTaskId());
        context.getParentTaskId().ifPresent(taskId -> {
            fields.field(PARENT_TASK_ID_FIELD, taskId.getId());
            fields.field(PARENT_NODE_ID_FIELD, taskId.getNodeId());
        });
        if (context.isSuccess() == false) {
            fields.field("error.type", context.getErrorType());
            fields.field("error.message", context.getErrorMessage());
        }
        if (context.isTimedOut()) {
            fields.field(prefix + "timed_out", true);
        }
        context.shardInfo().ifPresent(shardInfo -> {
            fields.field(QUERY_FIELD_SHARDS + "successful", shardInfo.successfulShards());
            if (shardInfo.skippedShards() != null && shardInfo.skippedShards() > 0) {
                fields.field(QUERY_FIELD_SHARDS + "skipped", shardInfo.skippedShards());
            }
            if (shardInfo.failedShards() != null && shardInfo.failedShards() > 0) {
                fields.field(QUERY_FIELD_SHARDS + "failed", shardInfo.failedShards());
            }
        });
        return fields;
    }
}
