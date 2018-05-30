/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class ShardFollowNodeTask extends AllocatedPersistentTask {

    private final AtomicLong processedGlobalCheckpoint = new AtomicLong();

    ShardFollowNodeTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    public boolean isRunning() {
        return isCancelled() == false && isCompleted() == false;
    }

    void updateProcessedGlobalCheckpoint(long processedGlobalCheckpoint) {
        this.processedGlobalCheckpoint.set(processedGlobalCheckpoint);
    }

    @Override
    public Task.Status getStatus() {
        return new Status(processedGlobalCheckpoint.get());
    }

    public static class Status implements Task.Status {

        public static final String NAME = "shard-follow-node-task-status";

        static final ParseField PROCESSED_GLOBAL_CHECKPOINT_FIELD = new ParseField("processed_global_checkpoint");

        static final ConstructingObjectParser<Status, Void> PARSER =
                new ConstructingObjectParser<>(NAME, args -> new Status((Long) args[0]));

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), PROCESSED_GLOBAL_CHECKPOINT_FIELD);
        }

        private final long processedGlobalCheckpoint;

        Status(long processedGlobalCheckpoint) {
            this.processedGlobalCheckpoint = processedGlobalCheckpoint;
        }

        public Status(StreamInput in) throws IOException {
            this.processedGlobalCheckpoint = in.readZLong();
        }

        public long getProcessedGlobalCheckpoint() {
            return processedGlobalCheckpoint;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(processedGlobalCheckpoint);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(PROCESSED_GLOBAL_CHECKPOINT_FIELD.getPreferredName(), processedGlobalCheckpoint);
            }
            builder.endObject();
            return builder;
        }

        public static Status fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return processedGlobalCheckpoint == status.processedGlobalCheckpoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(processedGlobalCheckpoint);
        }

        public String toString() {
            return Strings.toString(this);
        }
    }

}
