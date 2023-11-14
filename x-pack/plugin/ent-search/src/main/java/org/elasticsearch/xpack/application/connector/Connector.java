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
import java.util.Map;

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

    @Nullable
    private final Map<String, Object> configuration;

    @Nullable
    private final ConnectorCustomSchedule customScheduling;

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

    private final String lastAccessControlSyncError;

    private final String lastAccessControlSyncScheduledAt;

    private final ConnectorSyncStatus lastAccessControlStatus;

    private final String lastIncrementalSyncScheduledAt;

    private final String lastSeen;

    private final String lastSyncError;

    private final String lastSyncScheduledAt;

    private final ConnectorSyncStatus lastSyncStatus;

    private final String lastSynced;

    @Nullable
    private final String name;
    //
    // private final ConnectorIngestPipelineParams pipeline;
    //
    @Nullable
    private final ConnectorScheduling scheduling;
    //
    @Nullable
    private final String serviceType;

    @Nullable
    private final ConnectorStatus status;

    private final boolean syncNow;

    private Connector(
        String connectorId,
        String apiKeyId,
        Map<String, Object> configuration,
        ConnectorCustomSchedule customScheduling,
        String description,
        String error,
        String indexName,
        boolean isNative,
        String language,
        String lastAccessControlSyncError,
        String lastAccessControlSyncScheduledAt,
        ConnectorSyncStatus lastAccessControlStatus,
        String lastIncrementalSyncScheduledAt,
        String lastSeen,
        String lastSyncError,
        String lastSyncScheduledAt,
        ConnectorSyncStatus lastSyncStatus,
        String lastSynced,
        String name,
        ConnectorScheduling scheduling,
        String serviceType,
        ConnectorStatus status,
        boolean syncNow
    ) {
        this.connectorId = connectorId;
        this.apiKeyId = apiKeyId;
        this.configuration = configuration;
        this.customScheduling = customScheduling;
        this.description = description;
        this.error = error;
        this.indexName = indexName;
        this.isNative = isNative;
        this.language = language;
        this.lastAccessControlSyncError = lastAccessControlSyncError;
        this.lastAccessControlSyncScheduledAt = lastAccessControlSyncScheduledAt;
        this.lastAccessControlStatus = lastAccessControlStatus;
        this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
        this.lastSeen = lastSeen;
        this.lastSyncError = lastSyncError;
        this.lastSyncScheduledAt = lastSyncScheduledAt;
        this.lastSyncStatus = lastSyncStatus;
        this.lastSynced = lastSynced;
        this.name = name;
        this.scheduling = scheduling;
        this.serviceType = serviceType;
        this.status = status;
        this.syncNow = syncNow;
    }

    public Connector(StreamInput in) throws IOException {
        this.connectorId = in.readString();
        this.apiKeyId = in.readOptionalString();
        if (in.readBoolean()) {
            this.configuration = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        } else {
            this.configuration = null;
        }

        if (in.readBoolean()) {
            this.customScheduling = new ConnectorCustomSchedule(in);
        } else {
            this.customScheduling = null;
        }

        this.description = in.readOptionalString();
        this.error = in.readOptionalString();
        this.indexName = in.readOptionalString();

        this.isNative = in.readBoolean();

        this.language = in.readOptionalString();
        this.lastAccessControlSyncError = in.readOptionalString();
        this.lastAccessControlSyncScheduledAt = in.readOptionalString();

        if (in.readBoolean()) {
            this.lastAccessControlStatus = ConnectorSyncStatus.valueOf(in.readString());
        } else {
            this.lastAccessControlStatus = null;
        }

        this.lastIncrementalSyncScheduledAt = in.readOptionalString();
        this.lastSeen = in.readOptionalString();
        this.lastSyncError = in.readOptionalString();
        this.lastSyncScheduledAt = in.readOptionalString();

        if (in.readBoolean()) {
            this.lastSyncStatus = ConnectorSyncStatus.valueOf(in.readString());
        } else {
            this.lastSyncStatus = null;
        }

        this.lastSynced = in.readOptionalString();
        this.name = in.readOptionalString();

        if (in.readBoolean()) {
            this.scheduling = new ConnectorScheduling(in);
        } else {
            this.scheduling = null;
        }

        this.serviceType = in.readOptionalString();

        if (in.readBoolean()) {
            this.status = ConnectorStatus.valueOf(in.readString());
        } else {
            this.status = null;
        }

        this.syncNow = in.readBoolean();
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Connector, String> PARSER = new ConstructingObjectParser<>(
        "connector",
        false,
        (args, connectorId) -> new Builder().setConnectorId(connectorId)
            .setApiKeyId((String) args[0])
            .setConfiguration((Map<String, Object>) args[1])
            .setCustomScheduling((ConnectorCustomSchedule) args[2])
            .setDescription((String) args[3])
            .setError((String) args[4])
            .setIndexName((String) args[5])
            .setIsNative(args[6] != null && (boolean) args[6])
            .setLanguage((String) args[7])
            .setLastAccessControlSyncError((String) args[8])
            .setLastAccessControlSyncScheduledAt((String) args[9])
            .setLastAccessControlStatus((ConnectorSyncStatus) args[10])
            .setLastIncrementalSyncScheduledAt((String) args[11])
            .setLastSeen((String) args[12])
            .setLastSyncError((String) args[13])
            .setLastSyncScheduledAt((String) args[14])
            .setLastSyncStatus((ConnectorSyncStatus) args[15])
            .setLastSynced((String) args[16])
            .setName((String) args[17])
            .setScheduling((ConnectorScheduling) args[18])
            .setServiceType((String) args[19])
            .setStatus((ConnectorStatus) args[20])
            .setSyncNow(args[21] != null && (boolean) args[21])
            .createConnector()
    );

    public static final ParseField ID_FIELD = new ParseField("connector_id");
    public static final ParseField API_KEY_ID_FIELD = new ParseField("api_key_id");
    public static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");
    public static final ParseField CUSTOM_SCHEDULING_FIELD = new ParseField("custom_scheduling");
    public static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    public static final ParseField ERROR_FIELD = new ParseField("error");
    public static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    public static final ParseField IS_NATIVE_FIELD = new ParseField("is_native");
    public static final ParseField LANGUAGE_FIELD = new ParseField("language");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_ERROR = new ParseField("last_access_control_sync_error");
    public static final ParseField LAST_ACCESS_CONTROL_STATUS_FIELD = new ParseField("last_access_control_status");
    public static final ParseField LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_access_control_sync_scheduled_at");
    public static final ParseField LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_incremental_sync_scheduled_at");
    public static final ParseField LAST_SEEN_FIELD = new ParseField("last_seen");
    public static final ParseField LAST_SYNC_ERROR_FIELD = new ParseField("last_sync_error");
    public static final ParseField LAST_SYNC_SCHEDULED_AT_FIELD = new ParseField("last_sync_scheduled_at");
    public static final ParseField LAST_SYNC_STATUS_FIELD = new ParseField("last_sync_status");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField SCHEDULING_FIELD = new ParseField("scheduling");
    public static final ParseField SERVICE_TYPE_FIELD = new ParseField("service_type");
    public static final ParseField STATUS_FIELD = new ParseField("status");

    public static final ParseField SYNC_NOW_FIELD = new ParseField("sync_now");

    static {
        PARSER.declareString(optionalConstructorArg(), API_KEY_ID_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> parser.map(),
            CONFIGURATION_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorCustomSchedule.fromXContent(p),
            CUSTOM_SCHEDULING_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareString(optionalConstructorArg(), ERROR_FIELD);
        PARSER.declareString(optionalConstructorArg(), INDEX_NAME_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), IS_NATIVE_FIELD);
        PARSER.declareString(optionalConstructorArg(), LANGUAGE_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_ACCESS_CONTROL_SYNC_ERROR);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorSyncStatus.connectorSyncStatus(p.text()),
            LAST_ACCESS_CONTROL_STATUS_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(optionalConstructorArg(), LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_SEEN_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_SYNC_ERROR_FIELD);
        PARSER.declareString(optionalConstructorArg(), LAST_SYNC_SCHEDULED_AT_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorSyncStatus.connectorSyncStatus(p.text()),
            LAST_SYNC_STATUS_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(optionalConstructorArg(), LAST_SYNCED_FIELD);
        PARSER.declareString(optionalConstructorArg(), NAME_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorScheduling.fromXContent(p),
            SCHEDULING_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(optionalConstructorArg(), SERVICE_TYPE_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorStatus.connectorStatus(p.text()),
            STATUS_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(optionalConstructorArg(), SYNC_NOW_FIELD);
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
            if (configuration != null) {
                builder.field(CONFIGURATION_FIELD.getPreferredName(), configuration);
            }
            if (customScheduling != null) {
                builder.field(CUSTOM_SCHEDULING_FIELD.getPreferredName(), customScheduling);
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
            if (lastAccessControlSyncError != null) {
                builder.field(LAST_SYNC_ERROR_FIELD.getPreferredName(), lastAccessControlSyncError);
            }
            if (lastAccessControlStatus != null) {
                builder.field(LAST_ACCESS_CONTROL_STATUS_FIELD.getPreferredName(), lastAccessControlStatus.toString());
            }
            if (lastAccessControlSyncScheduledAt != null) {
                builder.field(LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastAccessControlSyncScheduledAt);
            }
            if (lastIncrementalSyncScheduledAt != null) {
                builder.field(LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastIncrementalSyncScheduledAt);
            }
            if (lastSeen != null) {
                builder.field(LAST_SEEN_FIELD.getPreferredName(), lastSeen);
            }
            if (lastSyncError != null) {
                builder.field(LAST_SYNC_ERROR_FIELD.getPreferredName(), lastSyncError);
            }
            if (lastSyncScheduledAt != null) {
                builder.field(LAST_SYNC_SCHEDULED_AT_FIELD.getPreferredName(), lastSyncScheduledAt);
            }
            if (lastSyncStatus != null) {
                builder.field(LAST_SYNC_STATUS_FIELD.getPreferredName(), lastSyncStatus.toString());
            }
            if (lastSynced != null) {
                builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
            }
            if (name != null) {
                builder.field(NAME_FIELD.getPreferredName(), name);
            }
            if (scheduling != null) {
                builder.field(SCHEDULING_FIELD.getPreferredName(), scheduling);
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
        out.writeBoolean(configuration != null);
        if (configuration != null) {
            out.writeMap(configuration, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
        out.writeBoolean(customScheduling != null);
        if (customScheduling != null) {
            customScheduling.writeTo(out);
        }
        out.writeOptionalString(description);
        out.writeOptionalString(error);
        out.writeOptionalString(indexName);
        out.writeBoolean(isNative);
        out.writeOptionalString(language);
        out.writeOptionalString(lastAccessControlSyncError);
        out.writeOptionalString(lastAccessControlSyncScheduledAt);
        out.writeBoolean(lastAccessControlStatus != null);
        if (lastAccessControlStatus != null) {
            out.writeString(lastAccessControlStatus.name());
        }
        out.writeOptionalString(lastIncrementalSyncScheduledAt);
        out.writeOptionalString(lastSeen);
        out.writeOptionalString(lastSyncError);
        out.writeOptionalString(lastSyncScheduledAt);
        out.writeBoolean(lastSyncStatus != null);
        if (lastSyncStatus != null) {
            out.writeString(lastSyncStatus.name());
        }
        out.writeOptionalString(lastSynced);
        out.writeOptionalString(name);
        out.writeBoolean(scheduling != null);
        if (scheduling != null) {
            scheduling.writeTo(out);
        }
        out.writeOptionalString(serviceType);
        out.writeBoolean(status != null);
        if (status != null) {
            out.writeString(status.name());
        }
        out.writeBoolean(syncNow);
    }

    public String id() {
        return connectorId;
    }

    public static class Builder {

        private String connectorId;
        private String apiKeyId;
        private Map<String, Object> configuration;
        private ConnectorCustomSchedule customScheduling;
        private String description;
        private String error;
        private String indexName;
        private boolean isNative;
        private String language;
        private String lastAccessControlSyncError;
        private String lastAccessControlSyncScheduledAt;
        private ConnectorSyncStatus lastAccessControlStatus;
        private String lastIncrementalSyncScheduledAt;
        private String lastSeen;
        private String lastSyncError;
        private String lastSyncScheduledAt;
        private ConnectorSyncStatus lastSyncStatus;
        private String lastSynced;
        private String name;
        private ConnectorScheduling scheduling;
        private String serviceType;
        private ConnectorStatus status;
        private boolean syncNow;

        public Builder setConnectorId(String connectorId) {
            this.connectorId = connectorId;
            return this;
        }

        public Builder setApiKeyId(String apiKeyId) {
            this.apiKeyId = apiKeyId;
            return this;
        }

        public Builder setConfiguration(Map<String, Object> configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder setCustomScheduling(ConnectorCustomSchedule customScheduling) {
            this.customScheduling = customScheduling;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setError(String error) {
            this.error = error;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setIsNative(boolean isNative) {
            this.isNative = isNative;
            return this;
        }

        public Builder setLanguage(String language) {
            this.language = language;
            return this;
        }

        public Builder setLastAccessControlSyncError(String lastAccessControlSyncError) {
            this.lastAccessControlSyncError = lastAccessControlSyncError;
            return this;
        }

        public Builder setLastAccessControlSyncScheduledAt(String lastAccessControlSyncScheduledAt) {
            this.lastAccessControlSyncScheduledAt = lastAccessControlSyncScheduledAt;
            return this;
        }

        public Builder setLastAccessControlStatus(ConnectorSyncStatus lastAccessControlStatus) {
            this.lastAccessControlStatus = lastAccessControlStatus;
            return this;
        }

        public Builder setLastIncrementalSyncScheduledAt(String lastIncrementalSyncScheduledAt) {
            this.lastIncrementalSyncScheduledAt = lastIncrementalSyncScheduledAt;
            return this;
        }

        public Builder setLastSeen(String lastSeen) {
            this.lastSeen = lastSeen;
            return this;
        }

        public Builder setLastSyncError(String lastSyncError) {
            this.lastSyncError = lastSyncError;
            return this;
        }

        public Builder setLastSyncScheduledAt(String lastSyncScheduledAt) {
            this.lastSyncScheduledAt = lastSyncScheduledAt;
            return this;
        }

        public Builder setLastSyncStatus(ConnectorSyncStatus lastSyncStatus) {
            this.lastSyncStatus = lastSyncStatus;
            return this;
        }

        public Builder setLastSynced(String lastSynced) {
            this.lastSynced = lastSynced;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setScheduling(ConnectorScheduling scheduling) {
            this.scheduling = scheduling;
            return this;
        }

        public Builder setServiceType(String serviceType) {
            this.serviceType = serviceType;
            return this;
        }

        public Builder setStatus(ConnectorStatus status) {
            this.status = status;
            return this;
        }

        public Builder setSyncNow(boolean syncNow) {
            this.syncNow = syncNow;
            return this;
        }

        public Connector createConnector() {
            return new Connector(
                connectorId,
                apiKeyId,
                configuration,
                customScheduling,
                description,
                error,
                indexName,
                isNative,
                language,
                lastAccessControlSyncError,
                lastAccessControlSyncScheduledAt,
                lastAccessControlStatus,
                lastIncrementalSyncScheduledAt,
                lastSeen,
                lastSyncError,
                lastSyncScheduledAt,
                lastSyncStatus,
                lastSynced,
                name,
                scheduling,
                serviceType,
                status,
                syncNow
            );
        }
    }
}
