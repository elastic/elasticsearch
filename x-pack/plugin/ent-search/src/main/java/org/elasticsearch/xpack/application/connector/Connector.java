/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A connector document consists of:
 * <ul>
 *     <li>...</li>
 * </ul>
 */
public class Connector implements Writeable, ToXContentObject {

    private final String connectorId;

    @Nullable
    private final String apiKeyId;

    // private final ConnectorConfiguration configuration;
    //
    // private final ConnectorCustomScheduling customScheduling;
    //

    @Nullable
    private final String description;

    @Nullable
    private final String error;
    //
    // private final ConnectorFeatures features;
    //
    // private final List<ConnectorFilteringConfig> filtering;

    @Nullable
    private final String indexName;

    private final boolean isNative;

    @Nullable
    private final String language;

    // private final String lastAccessControlSyncError;
    //
    // private final String lastAccessControlSyncScheduledAt;
    //
    // private final String lastAccessControlStatus;
    //
    // private final String lastIncrementalSyncScheduledAt;
    //
    // private final String lastSeen;
    //
    // private final String lastSyncError;
    //
    // private final String lastSyncError;
    //
    // private final String lastSyncScheduledAt;
    //
    // private final String lastSyncStatus;
    //
    // private final String lastSynced;
    @Nullable
    private final String name;
    //
    // private final ConnectorIngestPipelineParams pipeline;
    //
    // private final ConnectorSchedulingConfiguration scheduling;
    //
    @Nullable
    private final String serviceType;

    @Nullable
    private final ConnectorStatus status;

    // private final boolean syncNow;

    public enum ConnectorStatus {
        CREATED,
        NEEDS_CONFIGURATION,
        CONFIGURED,
        CONNECTED,
        ERROR;

        public static Connector.ConnectorStatus connectorStatus(String status) {
            for (Connector.ConnectorStatus connectorStatus : Connector.ConnectorStatus.values()) {
                if (connectorStatus.name().equalsIgnoreCase(status)) {
                    return connectorStatus;
                }
            }
            throw new IllegalArgumentException("Unknown ConnectorStatus: " + status);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public Connector(
        String connectorId,
        String apiKeyId,
        String description,
        String error,
        String indexName,
        boolean isNative,
        String language,
        String name,
        String serviceType,
        ConnectorStatus status
    ) {
        this.connectorId = connectorId;
        this.apiKeyId = apiKeyId;
        this.description = description;
        this.error = error;
        this.indexName = indexName;
        this.isNative = isNative;
        this.language = language;
        this.name = name;
        this.serviceType = serviceType;
        this.status = status;
    }

    public Connector(StreamInput in) throws IOException {
        this.connectorId = in.readString();
        this.apiKeyId = in.readOptionalString();
        this.description = in.readOptionalString();
        this.error = in.readOptionalString();
        this.indexName = in.readOptionalString();
        this.isNative = in.readBoolean();
        this.language = in.readOptionalString();
        this.name = in.readOptionalString();
        this.serviceType = in.readOptionalString();
        this.status = ConnectorStatus.connectorStatus(in.readOptionalString());
    }

    private static final ConstructingObjectParser<Connector, String> PARSER = new ConstructingObjectParser<>(
        "connector",
        false,
        (args, internalId) -> {

            final boolean isNative = args[4] != null && (boolean) args[4];

            return new Connector(
                internalId, // id
                (String) args[0], // apiKeyId
                (String) args[1], // description
                (String) args[2], // error
                (String) args[3], // indexName
                isNative, // isNative
                (String) args[5], // language
                (String) args[6], // name
                (String) args[7], // serviceType
                (ConnectorStatus) args[8] // status
            );
        }
    );

    public static final ParseField ID_FIELD = new ParseField("connector_id");
    public static final ParseField API_KEY_ID_FIELD = new ParseField("apiKeyId");
    public static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    public static final ParseField ERROR_FIELD = new ParseField("error");
    public static final ParseField INDEX_NAME_FIELD = new ParseField("indexName");
    public static final ParseField IS_NATIVE_FIELD = new ParseField("isNative");
    public static final ParseField LANGUAGE_FIELD = new ParseField("language");
    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField SERVICE_TYPE_FIELD = new ParseField("serviceType");
    public static final ParseField STATUS_FIELD = new ParseField("status");

    static {
        PARSER.declareString(optionalConstructorArg(), API_KEY_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareString(optionalConstructorArg(), ERROR_FIELD);
        PARSER.declareString(optionalConstructorArg(), INDEX_NAME_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), IS_NATIVE_FIELD);
        PARSER.declareString(optionalConstructorArg(), LANGUAGE_FIELD);
        PARSER.declareString(optionalConstructorArg(), NAME_FIELD);
        PARSER.declareString(optionalConstructorArg(), SERVICE_TYPE_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorStatus.connectorStatus(p.text()),
            STATUS_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    public static Connector fromXContentBytes(String connectorId, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return Connector.fromXContent(connectorId, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    public static Connector fromXContent(String connectorId, XContentParser parser) throws IOException {
        return PARSER.parse(parser, connectorId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), connectorId);
            if (apiKeyId != null) {
                builder.field(API_KEY_ID_FIELD.getPreferredName(), apiKeyId);
            }
            if (description != null) {
                builder.field(DESCRIPTION_FIELD.getPreferredName(), description);
            }
            if (error != null) {
                builder.field(ERROR_FIELD.getPreferredName(), error);
            }
            if (indexName != null) {
                builder.field(INDEX_NAME_FIELD.getPreferredName(), indexName);
            }
            builder.field(IS_NATIVE_FIELD.getPreferredName(), isNative);
            if (language != null) {
                builder.field(LANGUAGE_FIELD.getPreferredName(), language);
            }
            if (name != null) {
                builder.field(NAME_FIELD.getPreferredName(), name);
            }
            if (serviceType != null) {
                builder.field(SERVICE_TYPE_FIELD.getPreferredName(), serviceType);
            }
            if (status != null) {
                builder.field(STATUS_FIELD.getPreferredName(), status.toString());
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(connectorId);
        out.writeOptionalString(apiKeyId);
        out.writeOptionalString(description);
        out.writeOptionalString(error);
        out.writeOptionalString(indexName);
        out.writeBoolean(isNative);
        out.writeOptionalString(language);
        out.writeOptionalString(name);
        out.writeOptionalString(serviceType);
        out.writeOptionalString(status.name());
    }

    public String id() {
        return connectorId;
    }
}
