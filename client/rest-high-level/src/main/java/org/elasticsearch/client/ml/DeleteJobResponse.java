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
package org.elasticsearch.client.ml;

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

/**
 * Response object that contains the acknowledgement or the task id
 * depending on whether the delete job action was requested to wait for completion.
 */
public class DeleteJobResponse implements ToXContentObject {

    private static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");
    private static final ParseField TASK = new ParseField("task");

    public static final ConstructingObjectParser<DeleteJobResponse, Void> PARSER = new ConstructingObjectParser<>("delete_job_response",
            true, a-> new DeleteJobResponse((Boolean) a[0], (TaskId) a[1]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ACKNOWLEDGED);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), TaskId.parser(), TASK, ObjectParser.ValueType.STRING);
    }

    public static DeleteJobResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Boolean acknowledged;
    private final TaskId task;

    DeleteJobResponse(@Nullable Boolean acknowledged, @Nullable TaskId task) {
        assert acknowledged != null || task != null;
        this.acknowledged = acknowledged;
        this.task = task;
    }

    /**
     * Get the action acknowledgement
     * @return {@code null} when the request had {@link DeleteJobRequest#getWaitForCompletion()} set to {@code false} or
     * otherwise a {@code boolean} that indicates whether the job was deleted successfully.
     */
    public Boolean getAcknowledged() {
        return acknowledged;
    }

    /**
     * Get the task id
     * @return {@code null} when the request had {@link DeleteJobRequest#getWaitForCompletion()} set to {@code true} or
     * otherwise the id of the job deletion task.
     */
    public TaskId getTask() {
        return task;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, task);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DeleteJobResponse that = (DeleteJobResponse) other;
        return Objects.equals(acknowledged, that.acknowledged) && Objects.equals(task, that.task);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (acknowledged != null) {
            builder.field(ACKNOWLEDGED.getPreferredName(), acknowledged);
        }
        if (task != null) {
            builder.field(TASK.getPreferredName(), task.toString());
        }
        builder.endObject();
        return builder;
    }
}
