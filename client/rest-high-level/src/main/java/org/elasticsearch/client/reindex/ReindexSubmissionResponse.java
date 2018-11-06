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
package org.elasticsearch.client.reindex;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Objects;

public class ReindexSubmissionResponse extends ActionResponse implements ToXContentObject {
    private static final ParseField TASK = new ParseField("task");
    public static final ConstructingObjectParser<ReindexSubmissionResponse, Void> PARSER = new ConstructingObjectParser<>(
        "reindex_submission_response",
        true, a -> new ReindexSubmissionResponse((TaskId) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), TaskId.parser(), TASK, ObjectParser.ValueType.STRING);
    }

    public static ReindexSubmissionResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final TaskId task;

    ReindexSubmissionResponse(@Nullable TaskId task) {
        this.task = task;
    }

    /**
     * Get the task id
     *
     * @return the id of the reindex task.
     */
    public TaskId getTask() {
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
        ReindexSubmissionResponse that = (ReindexSubmissionResponse) other;
        return Objects.equals(task, that.task);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (task != null) {
            builder.field(TASK.getPreferredName(), task.toString());
        }
        builder.endObject();
        return builder;
    }
}
