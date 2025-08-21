/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GetDataStreamSettingsAction extends ActionType<GetDataStreamSettingsAction.Response> {
    public static final String NAME = "indices:monitor/data_stream/settings/get";
    public static final GetDataStreamSettingsAction INSTANCE = new GetDataStreamSettingsAction();

    public GetDataStreamSettingsAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        private String[] dataStreamNames;

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        @Override
        public GetDataStreamSettingsAction.Request indices(String... dataStreamNames) {
            this.dataStreamNames = dataStreamNames;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public String[] indices() {
            return dataStreamNames;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetDataStreamSettingsAction.Request request = (GetDataStreamSettingsAction.Request) o;
            return Arrays.equals(dataStreamNames, request.dataStreamNames);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(dataStreamNames);
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final List<DataStreamSettingsResponse> dataStreamSettingsResponses;

        public Response(List<DataStreamSettingsResponse> dataStreamSettingsResponses) {
            this.dataStreamSettingsResponses = dataStreamSettingsResponses;
        }

        public List<DataStreamSettingsResponse> getDataStreamSettingsResponses() {
            return dataStreamSettingsResponses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "This ought to never be called because this action only runs locally";
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                Iterators.single((builder, params1) -> builder.startObject().startArray("data_streams")),
                dataStreamSettingsResponses.stream().map(dataStreamSettingsResponse -> (ToXContent) dataStreamSettingsResponse).iterator(),
                Iterators.single((builder, params1) -> builder.endArray().endObject())
            );
        }
    }

    public record DataStreamSettingsResponse(String dataStreamName, Settings settings, Settings effectiveSettings) implements ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            builder.startObject("settings");
            settings.toXContent(builder, params);
            builder.endObject();
            builder.startObject("effective_settings");
            effectiveSettings.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }
    }

}
