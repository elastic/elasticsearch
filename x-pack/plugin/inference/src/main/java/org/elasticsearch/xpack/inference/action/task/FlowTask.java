/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.task;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class FlowTask extends CancellableTask {
    private final AtomicReference<FlowStatus> flowStatus = new AtomicReference<>(FlowStatus.CONNECTING);

    FlowTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public Status getStatus() {
        return flowStatus.get();
    }

    public void updateStatus(FlowStatus status) {
        flowStatus.set(status);
    }

    enum FlowStatus implements Task.Status {
        CONNECTING("Connecting"),
        CONNECTED("Connected");

        static final String NAME = "streaming_task_manager_flow_status";
        static final Reader<FlowStatus> STREAM_READER = in -> FlowStatus.valueOf(in.readString());

        private final String status;

        FlowStatus(String status) {
            this.status = status;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(status);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("status", status).endObject();
        }
    }
}
