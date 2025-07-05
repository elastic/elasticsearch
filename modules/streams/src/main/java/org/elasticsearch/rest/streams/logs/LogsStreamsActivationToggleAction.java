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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class LogsStreamsActivationToggleAction {

    public static ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("cluster:admin/streams/logs/toggle");

    public static class Request extends AcknowledgedRequest<Request> {

        private final boolean enable;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, boolean enable) {
            super(masterNodeTimeout, ackTimeout);
            this.enable = enable;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.enable = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(enable);
        }

        @Override
        public String toString() {
            return "LogsStreamsActivationToggleAction.Request{" + "enable=" + enable + '}';
        }

        public boolean shouldEnable() {
            return enable;
        }

        @Override
        public Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(taskId.getId(), type, action, "Logs streams activation toggle request", parentTaskId, headers);
        }
    }
}
