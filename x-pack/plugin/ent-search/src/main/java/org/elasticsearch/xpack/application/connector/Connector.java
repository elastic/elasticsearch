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
import java.util.Collections;
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

    private final Map<String, Object> configuration;

    private final ConnectorCustomSchedule customScheduling;
    @Nullable
    private final String description;
    @Nullable
    private final String error;
    @Nullable
    private final ConnectorFeatures features;

    // TODO: it's a pretty complex object, it will come as a separate PR
    // private final List<ConnectorFilteringConfig> filtering;

    @Nullable
    private final String indexName;
    @Nullable
    private final Boolean isNative;
    @Nullable
    private final String language;
    @Nullable
    private final ConnectorSyncInfo syncInfo;
    @Nullable
    private final String name;
    @Nullable
    private final ConnectorIngestPipeline pipeline;
    @Nullable
    private final ConnectorScheduling scheduling;
    @Nullable
    private final String serviceType;

    private final ConnectorStatus status;
    @Nullable
    private final Object syncCursor;

    private final Boolean syncNow;

    private Connector(
        String connectorId,
        String apiKeyId,
        Map<String, Object> configuration,
        ConnectorCustomSchedule customScheduling,
        String description,
        String error,
        ConnectorFeatures features,
        String indexName,
        Boolean isNative,
        String language,
        ConnectorSyncInfo syncInfo,
        String name,
        ConnectorIngestPipeline pipeline,
        ConnectorScheduling scheduling,
        String serviceType,
        ConnectorStatus status,
        Object syncCursor,
        Boolean syncNow
    ) {
        this.connectorId = connectorId;
        this.apiKeyId = apiKeyId;
        this.configuration = configuration;
        this.customScheduling = customScheduling;
        this.description = description;
        this.error = error;
        this.features = features;
        this.indexName = indexName;
        this.isNative = isNative;
        this.language = language;
        this.syncInfo = syncInfo;
        this.name = name;
        this.pipeline = pipeline;
        this.scheduling = scheduling;
        this.serviceType = serviceType;
        this.status = status;
        this.syncCursor = syncCursor;
        this.syncNow = syncNow;
    }

    public Connector(StreamInput in) throws IOException {
        this.connectorId = in.readString();
        this.apiKeyId = in.readOptionalString();
        this.configuration = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        this.customScheduling = in.readOptionalWriteable(ConnectorCustomSchedule::new);
        this.description = in.readOptionalString();
        this.error = in.readOptionalString();
        this.features = in.readOptionalWriteable(ConnectorFeatures::new);
        this.indexName = in.readOptionalString();
        this.isNative = in.readBoolean();
        this.language = in.readOptionalString();
        this.syncInfo = in.readOptionalWriteable(ConnectorSyncInfo::new);
        this.name = in.readOptionalString();
        this.pipeline = in.readOptionalWriteable(ConnectorIngestPipeline::new);
        this.scheduling = in.readOptionalWriteable(ConnectorScheduling::new);
        this.serviceType = in.readOptionalString();
        this.status = ConnectorStatus.valueOf(in.readString());
        this.syncCursor = in.readGenericValue();
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
            .setFeatures((ConnectorFeatures) args[5])
            .setIndexName((String) args[6])
            .setIsNative((Boolean) args[7])
            .setLanguage((String) args[8])
            .setSyncInfo((ConnectorSyncInfo) args[9])
            .setName((String) args[10])
            .setPipeline((ConnectorIngestPipeline) args[11])
            .setScheduling((ConnectorScheduling) args[12])
            .setServiceType((String) args[13])
            .setStatus((ConnectorStatus) args[14])
            .setSyncCursor(args[15])
            .setSyncNow((Boolean) args[26])
            .build()
    );

    public static final ParseField ID_FIELD = new ParseField("connector_id");
    public static final ParseField API_KEY_ID_FIELD = new ParseField("api_key_id");
    public static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");
    public static final ParseField CUSTOM_SCHEDULING_FIELD = new ParseField("custom_scheduling");
    public static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    public static final ParseField ERROR_FIELD = new ParseField("error");
    public static final ParseField FEATURES_FIELD = new ParseField("features");
    public static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    public static final ParseField IS_NATIVE_FIELD = new ParseField("is_native");
    public static final ParseField LANGUAGE_FIELD = new ParseField("language");

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField PIPELINE_FIELD = new ParseField("pipeline");
    public static final ParseField SCHEDULING_FIELD = new ParseField("scheduling");
    public static final ParseField SERVICE_TYPE_FIELD = new ParseField("service_type");
    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField SYNC_CURSOR_FIELD = new ParseField("sync_cursor");
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
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorFeatures.fromXContent(p),
            FEATURES_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(optionalConstructorArg(), INDEX_NAME_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), IS_NATIVE_FIELD);
        PARSER.declareString(optionalConstructorArg(), LANGUAGE_FIELD);
//        PARSER.declareField(
//            optionalConstructorArg(),
//            (p, c) -> ConnectorSyncInfo.fromXContent(p),
//            SYN,
//            ObjectParser.ValueType.OBJECT
//        );
        PARSER.declareString(optionalConstructorArg(), NAME_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorIngestPipeline.fromXContent(p),
            PIPELINE_FIELD,
            ObjectParser.ValueType.OBJECT
        );
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
        PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parser.map(),
            SYNC_CURSOR_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NULL
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
            if (features != null) {
                builder.field(FEATURES_FIELD.getPreferredName(), features);
            }
            if (indexName != null) {
                builder.field(INDEX_NAME_FIELD.getPreferredName(), indexName);
            }
            builder.field(IS_NATIVE_FIELD.getPreferredName(), isNative);
            if (language != null) {
                builder.field(LANGUAGE_FIELD.getPreferredName(), language);
            }
            if (syncInfo != null) {
                syncInfo.toXContent(builder, params);
            }
            if (name != null) {
                builder.field(NAME_FIELD.getPreferredName(), name);
            }
            if (pipeline != null) {
                builder.field(PIPELINE_FIELD.getPreferredName(), pipeline);
            }
            if (scheduling != null) {
                builder.field(SCHEDULING_FIELD.getPreferredName(), scheduling);
            }
            if (serviceType != null) {
                builder.field(SERVICE_TYPE_FIELD.getPreferredName(), serviceType);
            }
            if (syncCursor != null) {
                builder.field(SYNC_CURSOR_FIELD.getPreferredName(), syncCursor);
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
        out.writeMap(configuration, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeOptionalWriteable(customScheduling);
        out.writeOptionalString(description);
        out.writeOptionalString(error);
        out.writeOptionalWriteable(features);
        out.writeOptionalString(indexName);
        out.writeBoolean(isNative);
        out.writeOptionalString(language);
        out.writeOptionalWriteable(syncInfo);
        out.writeOptionalString(name);
        out.writeOptionalWriteable(pipeline);
        out.writeOptionalWriteable(scheduling);
        out.writeOptionalString(serviceType);
        out.writeString(status.name());
        out.writeGenericValue(syncCursor);
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
        private ConnectorFeatures features;
        private String indexName;
        private Boolean isNative;
        private String language;
        private ConnectorSyncInfo syncInfo;
        private String name;
        private ConnectorIngestPipeline pipeline;
        private ConnectorScheduling scheduling;
        private String serviceType;
        private ConnectorStatus status;
        private Object syncCursor;
        private Boolean syncNow;

        public Builder setConnectorId(String connectorId) {
            this.connectorId = connectorId;
            return this;
        }

        public Builder setApiKeyId(String apiKeyId) {
            this.apiKeyId = apiKeyId;
            return this;
        }

        public Builder setConfiguration(Map<String, Object> configuration) {
            this.configuration = (configuration != null) ? configuration : Collections.emptyMap();
            return this;
        }

        public Builder setCustomScheduling(ConnectorCustomSchedule customScheduling) {
            this.customScheduling = (customScheduling != null) ? customScheduling : new ConnectorCustomSchedule.Builder().build();
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

        public Builder setFeatures(ConnectorFeatures features) {
            this.features = features;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setIsNative(Boolean isNative) {
            this.isNative = (isNative != null) ? isNative : false;
            return this;
        }

        public Builder setLanguage(String language) {
            this.language = language;
            return this;
        }

        public Builder setSyncInfo(ConnectorSyncInfo syncInfo) {
            this.syncInfo = syncInfo;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setPipeline(ConnectorIngestPipeline pipeline) {
            this.pipeline = pipeline;
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

        public Builder setSyncCursor(Object syncCursor) {
            this.syncCursor = syncCursor;
            return this;
        }

        public Builder setSyncNow(Boolean syncNow) {
            this.syncNow = (syncNow != null) ? syncNow : false;
            return this;
        }

        public Connector build() {
            return new Connector(
                connectorId,
                apiKeyId,
                configuration,
                customScheduling,
                description,
                error,
                features,
                indexName,
                isNative,
                language,
                syncInfo,
                name,
                pipeline,
                scheduling,
                serviceType,
                status,
                syncCursor,
                syncNow
            );
        }
    }
}
