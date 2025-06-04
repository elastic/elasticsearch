/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;

public enum TaskType implements Writeable {
    TEXT_EMBEDDING,
    SPARSE_EMBEDDING,
    RERANK,
    COMPLETION,
    ANY {
        @Override
        public boolean isAnyOrSame(TaskType other) {
            return true;
        }
    },
    CHAT_COMPLETION;

    public static final String NAME = "task_type";

    public static TaskType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static TaskType fromStringOrStatusException(String name) {
        if (name == null) {
            throw new ElasticsearchStatusException("Task type must not be null", RestStatus.BAD_REQUEST);
        }

        try {
            TaskType taskType = TaskType.fromString(name);
            return Objects.requireNonNull(taskType);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("Unknown task_type [{}]", RestStatus.BAD_REQUEST, name);
        }
    }

    /**
     * Return true if the {@code other} is the {@link #ANY} type
     * or the same as this.
     * @param other The other
     * @return True if same or any.
     */
    public boolean isAnyOrSame(TaskType other) {
        return other == TaskType.ANY || other == this;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static TaskType fromStream(StreamInput in) throws IOException {
        return in.readEnum(TaskType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static String unsupportedTaskTypeErrorMsg(TaskType taskType, String serviceName) {
        return "The [" + serviceName + "] service does not support task type [" + taskType + "]";
    }

    /**
     * Copies a {@link EnumSet<TaskType>} if non-empty, otherwise returns an empty {@link EnumSet<TaskType>}. This is essentially the same
     * as {@link EnumSet#copyOf(EnumSet)}, except it does not throw for an empty set.
     * @param taskTypes task types to copy
     * @return a copy of the passed in {@link EnumSet<TaskType>}
     */
    public static EnumSet<TaskType> copyOf(EnumSet<TaskType> taskTypes) {
        return taskTypes.isEmpty() ? EnumSet.noneOf(TaskType.class) : EnumSet.copyOf(taskTypes);
    }
}
