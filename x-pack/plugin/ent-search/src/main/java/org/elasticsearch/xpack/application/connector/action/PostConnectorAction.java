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
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PostConnectorAction {

    public static final String NAME = "indices:data/write/xpack/connector/post";
    public static final ActionType<PostConnectorAction.Response> INSTANCE = new ActionType<>(NAME);

    private PostConnectorAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

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

        public Request(String description, String indexName, Boolean isNative, String language, String name, String serviceType) {
            this.description = description;
            this.indexName = indexName;
            this.isNative = isNative;
            this.language = language;
            this.name = name;
            this.serviceType = serviceType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.description = in.readOptionalString();
            this.indexName = in.readString();
            this.isNative = in.readOptionalBoolean();
            this.language = in.readOptionalString();
            this.name = in.readOptionalString();
            this.serviceType = in.readOptionalString();
        }

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "connector_put_request",
            false,
            (args) -> new Request(
                (String) args[0],
                (String) args[1],
                (Boolean) args[2],
                (String) args[3],
                (String) args[4],
                (String) args[5]
            )
        );

        static {
            PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
            PARSER.declareString(constructorArg(), new ParseField("index_name"));
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("is_native"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("language"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("name"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("service_type"));
        }

        public static Request fromXContentBytes(BytesReference source, XContentType xContentType) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return Request.fromXContent(parser);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
            out.writeOptionalString(description);
            out.writeString(indexName);
            out.writeOptionalBoolean(isNative);
            out.writeOptionalString(language);
            out.writeOptionalString(name);
            out.writeOptionalString(serviceType);
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
            return Objects.equals(description, request.description)
                && Objects.equals(indexName, request.indexName)
                && Objects.equals(isNative, request.isNative)
                && Objects.equals(language, request.language)
                && Objects.equals(name, request.name)
                && Objects.equals(serviceType, request.serviceType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(description, indexName, isNative, language, name, serviceType);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final String id;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
        }

        public Response(String id) {
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        public String getId() {
            return id;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.ID_FIELD.getPreferredName(), id);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(id, response.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
