/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class TaskSubmissionResponse {

    private static final ParseField TASK = new ParseField("task");

    public static final ConstructingObjectParser<TaskSubmissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "task_submission_response",
        true, a -> new TaskSubmissionResponse((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TASK);
    }

    private final String task;

    TaskSubmissionResponse(String task) {
        this.task = task;
    }

    /**
     * Get the task id
     *
     * @return the id of the reindex task.
     */
    public String getTask() {
        return task;
    }

    @Override
    public int hashCode() {
        return Objects.hash(task);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        TaskSubmissionResponse that = (TaskSubmissionResponse) other;
        return Objects.equals(task, that.task);
    }

    public static TaskSubmissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
