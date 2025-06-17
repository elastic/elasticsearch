/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams.logs;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class StreamsStatusAction {

    public static ActionType<Response> INSTANCE = new ActionType<>("cluster:admin/streams/status");

    public static class Request extends LocalClusterStateRequest {
        protected Request(TimeValue masterTimeout) {
            super(masterTimeout);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "Streams status request", parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final boolean logs_enabled;

        public Response(boolean logsEnabled) {
            logs_enabled = logsEnabled;
        }

        public Response(StreamInput in) throws IOException {
            logs_enabled = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(logs_enabled);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startObject("logs");
            builder.field("enabled", logs_enabled);
            builder.endObject();

            builder.endObject();
            return builder;
        }
    }
}
