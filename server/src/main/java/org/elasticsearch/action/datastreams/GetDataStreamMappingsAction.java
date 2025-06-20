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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GetDataStreamMappingsAction extends ActionType<GetDataStreamMappingsAction.Response> {
    public static final String NAME = "indices:monitor/data_stream/mappings/get";
    public static final GetDataStreamMappingsAction INSTANCE = new GetDataStreamMappingsAction();

    public GetDataStreamMappingsAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        private String[] dataStreamNames;

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        @Override
        public GetDataStreamMappingsAction.Request indices(String... dataStreamNames) {
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
            GetDataStreamMappingsAction.Request request = (GetDataStreamMappingsAction.Request) o;
            return Arrays.equals(dataStreamNames, request.dataStreamNames);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(dataStreamNames);
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final List<DataStreamMappingsResponse> DataStreamMappingsResponses;

        public Response(List<DataStreamMappingsResponse> DataStreamMappingsResponses) {
            this.DataStreamMappingsResponses = DataStreamMappingsResponses;
        }

        public List<DataStreamMappingsResponse> getDataStreamMappingsResponses() {
            return DataStreamMappingsResponses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "This ought to never be called because this action only runs locally";
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                Iterators.single((builder, params1) -> builder.startObject().startArray("data_streams")),
                DataStreamMappingsResponses.stream().map(DataStreamMappingsResponse -> (ToXContent) DataStreamMappingsResponse).iterator(),
                Iterators.single((builder, params1) -> builder.endArray().endObject())
            );
        }
    }

    public record DataStreamMappingsResponse(String dataStreamName, CompressedXContent mappings, CompressedXContent effectiveMappings)
        implements
            ToXContent {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            Map<String, Object> uncompressedMappings = XContentHelper.convertToMap(mappings.uncompressed(), true, XContentType.JSON).v2();
            builder.field("mappings");
            builder.map(uncompressedMappings);
            Map<String, Object> uncompressedEffectiveMappings = XContentHelper.convertToMap(
                effectiveMappings.uncompressed(),
                true,
                XContentType.JSON
            ).v2();
            builder.field("effective_mappings");
            builder.map(uncompressedEffectiveMappings);
            builder.endObject();
            return builder;
        }
    }

}
