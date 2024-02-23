/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PutConnectorAction {

    public static final String NAME = "indices:data/write/xpack/connector/put";
    public static final ActionType<PutConnectorAction.Response> INSTANCE = new ActionType<>(NAME);

    private PutConnectorAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements IndicesRequest, ToXContentObject {

        private final String connectorId;

        @Nullable
        private final String description;
        private final String indexName;
        @Nullable
        private final Boolean isNative;
        @Nullable
        private final String language;
        @Nullable
        private final String name;
        @Nullable
        private final String serviceType;

        public Request(
            String connectorId,
            String description,
            String indexName,
            Boolean isNative,
            String language,
            String name,
            String serviceType
        ) {
            this.connectorId = connectorId;
            this.description = description;
            this.indexName = indexName;
            this.isNative = isNative;
            this.language = language;
            this.name = name;
            this.serviceType = serviceType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.description = in.readOptionalString();
            this.indexName = in.readString();
            this.isNative = in.readOptionalBoolean();
            this.language = in.readOptionalString();
            this.name = in.readOptionalString();
            this.serviceType = in.readOptionalString();
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "connector_put_request",
            false,
            ((args, connectorId) -> new Request(
                connectorId,
                (String) args[0],
                (String) args[1],
                (Boolean) args[2],
                (String) args[3],
                (String) args[4],
                (String) args[5]
            ))
        );

        static {
            PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("index_name"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("is_native"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("language"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("name"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("service_type"));
        }

        public static Request fromXContentBytes(String connectorId, BytesReference source, XContentType xContentType) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                if (description != null) {
                    builder.field("description", description);
                }
                builder.field("index_name", indexName);
                if (isNative != null) {
                    builder.field("is_native", isNative);
                }
                if (language != null) {
                    builder.field("language", language);
                }
                if (name != null) {
                    builder.field("name", name);
                }
                if (serviceType != null) {
                    builder.field("service_type", serviceType);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {

            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(getConnectorId())) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"]", validationException);
            }
            if (Strings.isNullOrEmpty(getIndexName())) {
                validationException = addValidationError("[index_name] cannot be [null] or [\"\"]", validationException);
            }
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(getIndexName(), InvalidIndexNameException::new);
            } catch (InvalidIndexNameException e) {
                validationException = addValidationError(e.toString(), validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalString(description);
            out.writeString(indexName);
            out.writeOptionalBoolean(isNative);
            out.writeOptionalString(language);
            out.writeOptionalString(name);
            out.writeOptionalString(serviceType);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public String getDescription() {
            return description;
        }

        public String getIndexName() {
            return indexName;
        }

        public Boolean getIsNative() {
            return isNative;
        }

        public String getLanguage() {
            return language;
        }

        public String getName() {
            return name;
        }

        public String getServiceType() {
            return serviceType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId)
                && Objects.equals(description, request.description)
                && Objects.equals(indexName, request.indexName)
                && Objects.equals(isNative, request.isNative)
                && Objects.equals(language, request.language)
                && Objects.equals(name, request.name)
                && Objects.equals(serviceType, request.serviceType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, description, indexName, isNative, language, name, serviceType);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        final DocWriteResponse.Result result;

        public Response(StreamInput in) throws IOException {
            super(in);
            result = DocWriteResponse.Result.readFrom(in);
        }

        public Response(DocWriteResponse.Result result) {
            this.result = result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", this.result.getLowercase());
            builder.endObject();
            return builder;
        }

        public RestStatus status() {
            return switch (result) {
                case CREATED -> RestStatus.CREATED;
                case NOT_FOUND -> RestStatus.NOT_FOUND;
                default -> RestStatus.OK;
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return result == response.result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }
}
