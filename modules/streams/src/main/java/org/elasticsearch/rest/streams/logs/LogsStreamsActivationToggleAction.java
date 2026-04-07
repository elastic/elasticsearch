/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.streams.StreamType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.StreamsMetadata.STREAM_SPLIT_VERSION;

public class LogsStreamsActivationToggleAction {

    public static ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("cluster:admin/streams/logs/toggle");

    public static class Request extends AcknowledgedRequest<Request> {

        private final boolean enable;
        private final StreamType stream;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, boolean enable, StreamType stream) {
            super(masterNodeTimeout, ackTimeout);
            this.enable = enable;
            this.stream = Objects.requireNonNull(stream, "stream type must not be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.enable = in.readBoolean();
            if (in.getTransportVersion().supports(STREAM_SPLIT_VERSION)) {
                this.stream = in.readEnum(StreamType.class);
            } else {
                // Treat "unspecified" as toggling all streams
                this.stream = StreamType.LOGS;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(enable);
            if (out.getTransportVersion().supports(STREAM_SPLIT_VERSION)) {
                out.writeEnum(stream);
            }
        }

        @Override
        public String toString() {
            return "LogsStreamsActivationToggleAction.Request{" + "enable=" + enable + ",stream=" + stream + "}";
        }

        public boolean shouldEnable() {
            return enable;
        }

        public StreamType stream() {
            return this.stream;
        }

        @Override
        public Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(taskId.getId(), type, action, "Logs streams activation toggle request", parentTaskId, headers);
        }
    }
}
