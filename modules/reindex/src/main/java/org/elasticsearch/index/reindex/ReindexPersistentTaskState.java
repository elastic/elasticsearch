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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public class ReindexPersistentTaskState implements Task.Status, PersistentTaskState {

    public static final String NAME = ReindexTask.NAME;

    public static final ConstructingObjectParser<ReindexPersistentTaskState, Void> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ReindexPersistentTaskState((String) a[0], (String) a[1]));

    private static String EPHEMERAL_TASK_ID = "ephemeral_task_id";
    private static String STATUS = "status";

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(),  new ParseField(EPHEMERAL_TASK_ID));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField(STATUS));
    }

    private final TaskId ephemeralTaskId;
    private final Status status;

    private ReindexPersistentTaskState(String ephemeralTaskId, String status) {
        this(new TaskId(ephemeralTaskId), Status.valueOf(status));
    }

    ReindexPersistentTaskState(TaskId ephemeralTaskId, Status status) {
        assert status != null : "Status cannot be null";
        this.ephemeralTaskId = ephemeralTaskId;
        this.status = status;
    }

    public ReindexPersistentTaskState(StreamInput in) throws IOException {
        ephemeralTaskId = TaskId.readFromStream(in);
        status = in.readEnum(Status.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ephemeralTaskId.writeTo(out);
        out.writeEnum(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(EPHEMERAL_TASK_ID, ephemeralTaskId.toString());
        builder.field(STATUS, status);
        return builder.endObject();
    }

    public boolean isDone() {
        return status != Status.STARTED;
    }

    public Status getStatus() {
        return status;
    }

    public TaskId getEphemeralTaskId() {
        return ephemeralTaskId;
    }

    public static ReindexPersistentTaskState fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public enum Status {
        STARTED,
        ASSIGNMENT_FAILED,
        DONE
    }
}
