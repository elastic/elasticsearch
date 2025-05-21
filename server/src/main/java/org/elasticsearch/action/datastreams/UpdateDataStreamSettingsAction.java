/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.Objects;

public class UpdateDataStreamSettingsAction extends ActionType<UpdateDataStreamSettingsAction.Response> {

    public static final String NAME = "indices:admin/data_stream/settings/update";
    public static final UpdateDataStreamSettingsAction INSTANCE = new UpdateDataStreamSettingsAction();

    public UpdateDataStreamSettingsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private final Settings settings;
        private String[] dataStreamNames = Strings.EMPTY_ARRAY;
        private final boolean dryRun;

        public Request(Settings settings, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            this(settings, false, masterNodeTimeout, ackTimeout);
        }

        public Request(Settings settings, boolean dryRun, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.settings = settings;
            this.dryRun = dryRun;
        }

        @Override
        public Request indices(String... dataStreamNames) {
            this.dataStreamNames = dataStreamNames;
            return this;
        }

        public Settings getSettings() {
            return settings;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStreamNames = in.readStringArray();
            this.settings = Settings.readSettingsFromStream(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.SETTINGS_IN_DATA_STREAMS)) {
                this.dryRun = in.readBoolean();
            } else {
                this.dryRun = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(dataStreamNames);
            settings.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.SETTINGS_IN_DATA_STREAMS_DRY_RUN)) {
                out.writeBoolean(dryRun);
            }
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
                && settings.equals(request.settings)
                && dryRun == request.dryRun
                && Objects.equals(masterNodeTimeout(), request.masterNodeTimeout())
                && Objects.equals(ackTimeout(), request.ackTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(dataStreamNames), settings, dryRun, masterNodeTimeout(), ackTimeout());
        }

    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final List<DataStreamSettingsResponse> dataStreamSettingsResponses;

        public Response(List<DataStreamSettingsResponse> dataStreamSettingsResponses) {
            this.dataStreamSettingsResponses = dataStreamSettingsResponses;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readCollectionAsList(DataStreamSettingsResponse::new));
        }

        public List<DataStreamSettingsResponse> getDataStreamSettingsResponses() {
            return dataStreamSettingsResponses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreamSettingsResponses, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                Iterators.single((builder, params1) -> builder.startObject().startArray("data_streams")),
                dataStreamSettingsResponses.stream().map(dataStreamSettingsResponse -> (ToXContent) dataStreamSettingsResponse).iterator(),
                Iterators.single((builder, params1) -> builder.endArray().endObject())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(dataStreamSettingsResponses, response.dataStreamSettingsResponses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreamSettingsResponses);
        }
    }

    public record DataStreamSettingsResponse(
        String dataStreamName,
        boolean dataStreamSucceeded,
        String dataStreamErrorMessage,
        Settings settings,
        Settings effectiveSettings,
        IndicesSettingsResult indicesSettingsResult
    ) implements ToXContent, Writeable {

        public DataStreamSettingsResponse(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readBoolean(),
                in.readOptionalString(),
                Settings.readSettingsFromStream(in),
                Settings.readSettingsFromStream(in),
                new IndicesSettingsResult(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStreamName);
            out.writeBoolean(dataStreamSucceeded);
            out.writeOptionalString(dataStreamErrorMessage);
            settings.writeTo(out);
            effectiveSettings.writeTo(out);
            indicesSettingsResult.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            builder.field("applied_to_data_stream", dataStreamSucceeded);
            if (dataStreamErrorMessage != null) {
                builder.field("error", dataStreamErrorMessage);
            }
            builder.startObject("settings");
            settings.toXContent(builder, params);
            builder.endObject();
            builder.startObject("effective_settings");
            effectiveSettings.toXContent(builder, params);
            builder.endObject();
            builder.startObject("index_settings_results");
            indicesSettingsResult.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        public record IndicesSettingsResult(
            List<String> appliedToDataStreamOnly,
            List<String> appliedToDataStreamAndBackingIndices,
            List<IndexSettingError> indexSettingErrors
        ) implements ToXContent, Writeable {

            public static final IndicesSettingsResult EMPTY = new IndicesSettingsResult(List.of(), List.of(), List.of());

            public IndicesSettingsResult(StreamInput in) throws IOException {
                this(in.readStringCollectionAsList(), in.readStringCollectionAsList(), in.readCollectionAsList(IndexSettingError::new));
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("applied_to_data_stream_only", appliedToDataStreamOnly);
                builder.field("applied_to_data_stream_and_backing_indices", appliedToDataStreamAndBackingIndices);
                if (indexSettingErrors.isEmpty() == false) {
                    builder.field("errors", indexSettingErrors);
                }
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeStringCollection(appliedToDataStreamOnly);
                out.writeStringCollection(appliedToDataStreamAndBackingIndices);
                out.writeCollection(indexSettingErrors, (out1, value) -> value.writeTo(out1));
            }
        }

        public record IndexSettingError(String indexName, String errorMessage) implements ToXContent, Writeable {
            public IndexSettingError(StreamInput in) throws IOException {
                this(in.readString(), in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(indexName);
                out.writeString(errorMessage);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("index", indexName);
                builder.field("error", errorMessage);
                builder.endObject();
                return builder;
            }
        }
    }
}
