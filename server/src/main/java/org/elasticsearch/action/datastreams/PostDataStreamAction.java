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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PostDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final PostDataStreamAction INSTANCE = new PostDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/update";

    private PostDataStreamAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String name;
        private final ComposableIndexTemplate templateOverrides;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name, ComposableIndexTemplate templateOverrides) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
            this.templateOverrides = templateOverrides;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        public String getName() {
            return name;
        }

        public ComposableIndexTemplate getTemplateOverrides() {
            return templateOverrides;
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
            this.templateOverrides = in.readOptionalWriteable(ComposableIndexTemplate::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeOptionalWriteable(templateOverrides);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) && templateOverrides.equals(request.templateOverrides);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, templateOverrides);
        }

        @Override
        public String[] indices() {
            return new String[] { name };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final List<DataStreamResponse> dataStreamResponses;

        public Response(List<DataStreamResponse> dataStreamResponses) {
            this.dataStreamResponses = dataStreamResponses;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readCollectionAsList(DataStreamResponse::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(dataStreamResponses, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("data_streams");
            for (DataStreamResponse dataStreamResponse : dataStreamResponses) {
                dataStreamResponse.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    public record DataStreamResponse(
        String dataStreamName,
        boolean dataStreamSucceeded,
        String dataStreamErrorMessage,
        ComposableIndexTemplate effectiveTemplate,
        List<IndexSettingResult> indexSettingResults
    ) implements ToXContent, Writeable {

        public DataStreamResponse(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readBoolean(),
                in.readOptionalString(),
                in.readOptionalWriteable(ComposableIndexTemplate::new),
                in.readCollectionAsList(IndexSettingResult::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(dataStreamName);
            out.writeBoolean(dataStreamSucceeded);
            out.writeOptionalString(dataStreamErrorMessage);
            out.writeOptionalWriteable(effectiveTemplate);
            out.writeCollection(indexSettingResults, (out1, value) -> value.writeTo(out1));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", dataStreamName);
            builder.field("applied_to_data_stream", dataStreamSucceeded);
            if (dataStreamErrorMessage != null) {
                builder.field("error", dataStreamErrorMessage);
            }
            builder.field("effective_template", effectiveTemplate);
            builder.field("index_settings_results", indexSettingResults);
            builder.endObject();
            return builder;
        }

        public record IndexSettingResult(String name, boolean appliedToBackingIndices, List<IndexSettingError> indexSettingErrors)
            implements
                ToXContent,
                Writeable {

            public IndexSettingResult(StreamInput in) throws IOException {
                this(in.readString(), in.readBoolean(), in.readCollectionAsList(IndexSettingError::new));
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("name", name);
                builder.field("applied_to_backing_indices", appliedToBackingIndices);
                if (indexSettingErrors.isEmpty() == false) {
                    builder.field("errors", indexSettingErrors);
                }
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeBoolean(appliedToBackingIndices);
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
