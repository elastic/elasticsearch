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
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.Objects;

public class UpdateDataStreamMappingsAction extends ActionType<UpdateDataStreamMappingsAction.Response> {

    public static final String NAME = "indices:admin/data_stream/mappings/update";
    public static final UpdateDataStreamMappingsAction INSTANCE = new UpdateDataStreamMappingsAction();

    public UpdateDataStreamMappingsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private final CompressedXContent mappings;
        private final boolean dryRun;
        private String[] dataStreamNames = Strings.EMPTY_ARRAY;

        public Request(CompressedXContent mappings, boolean dryRun, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.mappings = mappings;
            this.dryRun = dryRun;
        }

        @Override
        public Request indices(String... dataStreamNames) {
            this.dataStreamNames = dataStreamNames;
            return this;
        }

        public CompressedXContent getMappings() {
            return mappings;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStreamNames = in.readStringArray();
            this.mappings = CompressedXContent.readCompressedString(in);
            this.dryRun = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(dataStreamNames);
            mappings.writeTo(out);
            out.writeBoolean(dryRun);
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
            Request request = (Request) o;
            return Arrays.equals(dataStreamNames, request.dataStreamNames)
                && mappings.equals(request.mappings)
                && dryRun == request.dryRun
                && Objects.equals(masterNodeTimeout(), request.masterNodeTimeout())
                && Objects.equals(ackTimeout(), request.ackTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(dataStreamNames), mappings, dryRun, masterNodeTimeout(), ackTimeout());
        }

    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final List<DataStreamMappingsResponse> dataStreamMappingsResponses;

        public Response(List<DataStreamMappingsResponse> dataStreamMappingsResponses) {
            this.dataStreamMappingsResponses = dataStreamMappingsResponses;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readCollectionAsList(DataStreamMappingsResponse::new));
        }

        public List<DataStreamMappingsResponse> getDataStreamMappingsResponses() {
            return dataStreamMappingsResponses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreamMappingsResponses, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                Iterators.single((builder, params1) -> builder.startObject().startArray("data_streams")),
                dataStreamMappingsResponses.stream().map(dataStreamMappingsResponse -> (ToXContent) dataStreamMappingsResponse).iterator(),
                Iterators.single((builder, params1) -> builder.endArray().endObject())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(dataStreamMappingsResponses, response.dataStreamMappingsResponses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreamMappingsResponses);
        }
    }

    public record DataStreamMappingsResponse(
        String dataStreamName,
        boolean dataStreamSucceeded,
        String dataStreamErrorMessage,
        CompressedXContent mappings,
        CompressedXContent effectiveMappings
    ) implements ToXContent, Writeable {

        public DataStreamMappingsResponse(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readBoolean(),
                in.readOptionalString(),
                CompressedXContent.readCompressedString(in),
                CompressedXContent.readCompressedString(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStreamName);
            out.writeBoolean(dataStreamSucceeded);
            out.writeOptionalString(dataStreamErrorMessage);
            mappings.writeTo(out);
            effectiveMappings.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            builder.field("applied_to_data_stream", dataStreamSucceeded);
            if (dataStreamErrorMessage != null) {
                builder.field("error", dataStreamErrorMessage);
            }
            Map<String, Object> uncompressedMappings = XContentHelper.convertToMap(mappings.uncompressed(), true, XContentType.JSON).v2();
            if (uncompressedMappings.isEmpty() == false) {
                builder.field("mappings");
                builder.map(uncompressedMappings);
            }
            Map<String, Object> uncompressedEffectiveMappings = XContentHelper.convertToMap(
                effectiveMappings.uncompressed(),
                true,
                XContentType.JSON
            ).v2();
            if (uncompressedEffectiveMappings.isEmpty() == false) {
                builder.field("effective_mappings");
                builder.map(uncompressedEffectiveMappings);
            }
            builder.endObject();
            return builder;
        }
    }
}
