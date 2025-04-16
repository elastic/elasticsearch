/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PutDataStreamSettingsAction extends ActionType<PutDataStreamSettingsAction.Response> {

    public static final String NAME = "indices:admin/data_stream/settings";
    public static final PutDataStreamSettingsAction INSTANCE = new PutDataStreamSettingsAction();

    public PutDataStreamSettingsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {
        private final String name;
        private final Settings settings;

        public Request(String name, Settings settings, TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.settings = settings;
        }

        public String getName() {
            return name;
        }

        public Settings getSettings() {
            return settings;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.settings = Settings.readSettingsFromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            settings.writeTo(out);
        }

        @Override
        public String[] indices() {
            return new String[] { name };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && settings.equals(request.settings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, settings);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final List<DataStreamSettingsResponse> dataStreamSettingsRespons;

        public Response(List<DataStreamSettingsResponse> dataStreamSettingsRespons) {
            this.dataStreamSettingsRespons = dataStreamSettingsRespons;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readCollectionAsList(DataStreamSettingsResponse::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreamSettingsRespons, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startArray("data_streams");
            for (DataStreamSettingsResponse dataStreamSettingsResponse : dataStreamSettingsRespons) {
                dataStreamSettingsResponse.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
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
