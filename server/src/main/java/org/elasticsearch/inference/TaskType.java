/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public enum TaskType implements Writeable {
    TEXT_EMBEDDING,
    SPARSE_EMBEDDING;

    public static String NAME = "task_type";

    public static TaskType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static TaskType fromStringOrStatusException(String name) {
        try {
            TaskType taskType = TaskType.fromString(name);
            return Objects.requireNonNull(taskType);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("Unknown task_type [{}]", RestStatus.BAD_REQUEST, name);
        }
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
}
