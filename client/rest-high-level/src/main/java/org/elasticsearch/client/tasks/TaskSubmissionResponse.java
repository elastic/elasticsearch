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

package org.elasticsearch.client.tasks;

import org.elasticsearch.common.ParseField;
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
