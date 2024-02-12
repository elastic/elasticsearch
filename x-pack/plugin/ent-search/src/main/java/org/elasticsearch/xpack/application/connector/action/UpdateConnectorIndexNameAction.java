/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
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

public class UpdateConnectorIndexNameAction {

    public static final String NAME = "indices:data/write/xpack/connector/update_index_name";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorIndexNameAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final String indexName;

        public Request(String connectorId, String indexName) {
            this.connectorId = connectorId;
            this.indexName = indexName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.indexName = in.readString();
        }

        public String getConnectorId() {
            return connectorId;
        }

        public String getIndexName() {
            return indexName;
        }

        private static final ConstructingObjectParser<UpdateConnectorIndexNameAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_index_name_request",
                false,
                ((args, connectorId) -> new UpdateConnectorIndexNameAction.Request(connectorId, (String) args[0]))
            );

        static {
            PARSER.declareString(constructorArg(), Connector.INDEX_NAME_FIELD);
        }

        public static UpdateConnectorIndexNameAction.Request fromXContentBytes(
            String connectorId,
            BytesReference source,
            XContentType xContentType
        ) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return UpdateConnectorIndexNameAction.Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static UpdateConnectorIndexNameAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.INDEX_NAME_FIELD.getPreferredName(), indexName);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (Strings.isNullOrEmpty(indexName)) {
                validationException = addValidationError("[index_name] cannot be [null] or [\"\"].", validationException);
            }

            try {
                MetadataCreateIndexService.validateIndexOrAliasName(indexName, InvalidIndexNameException::new);
            } catch (InvalidIndexNameException e) {
                validationException = addValidationError(e.toString(), validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeString(indexName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(indexName, request.indexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, indexName);
        }
    }
}
