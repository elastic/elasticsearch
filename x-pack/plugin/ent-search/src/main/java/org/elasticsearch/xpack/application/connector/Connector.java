/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a Connector in the Elasticsearch ecosystem. Connectors are used for integrating
 * and synchronizing external data sources with Elasticsearch. Each Connector instance encapsulates
 * various settings and state information, including:
 * <ul>
 *     <li>A unique identifier for distinguishing different connectors.</li>
 *     <li>API key for authenticating with Elasticsearch, ensuring secure access.</li>
 *     <li>A configuration mapping which holds specific settings and parameters for the connector's operation.</li>
 *     <li>A {@link ConnectorCustomSchedule} object that defines custom scheduling.</li>
 *     <li>A description providing an overview or purpose of the connector.</li>
 *     <li>An error string capturing the latest error encountered during the connector's operation, if any.</li>
 *     <li>A {@link ConnectorFeatures} object encapsulating the set of features enabled for this connector.</li>
 *     <li>A list of {@link ConnectorFiltering} objects for applying filtering rules to the data processed by the connector.</li>
 *     <li>The name of the Elasticsearch index where the synchronized data is stored or managed.</li>
 *     <li>A boolean flag 'isNative' indicating whether the connector is a native Elasticsearch connector.</li>
 *     <li>The language associated with the connector.</li>
 *     <li>A {@link ConnectorSyncInfo} object containing synchronization state and history information.</li>
 *     <li>The name of the connector.</li>
 *     <li>A {@link ConnectorIngestPipeline} object specifying the data ingestion pipeline configuration.</li>
 *     <li>A {@link ConnectorScheduling} object with the scheduling configuration to trigger data sync.</li>
 *     <li>The type of connector.</li>
 *     <li>A {@link ConnectorStatus} indicating the current status of the connector.</li>
 *     <li>A sync cursor, used for incremental syncs.</li>
 *     <li>A boolean flag 'syncNow', which, when set, triggers an immediate synchronization operation.</li>
 * </ul>
 */
public class Connector implements Writeable, ToXContentObject {

    private final String connectorId;
    @Nullable
    private final String apiKeyId;
    @Nullable
    private final Map<String, Object> configuration; // TODO: add explicit types
    @Nullable
    private final ConnectorCustomSchedule customScheduling;
    @Nullable
    private final String description;
    @Nullable
    private final String error;
    @Nullable
    private final ConnectorFeatures features;
    @Nullable
    private final List<ConnectorFiltering> filtering;
    @Nullable
    private final String indexName;

    private final boolean isNative;
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
    private final boolean syncNow;

    /**
     * Constructor for Connector.
     *
     * @param connectorId        Unique identifier for the connector.
     * @param apiKeyId           API key ID used for authentication/authorization against ES.
     * @param configuration      Configuration settings for the connector.
     * @param customScheduling   Custom scheduling settings for the connector.
     * @param description        Description of the connector.
     * @param error              Information about the last error encountered by the connector, if any.
     * @param features           Features enabled for the connector.
     * @param filtering          Filtering settings applied by the connector.
     * @param indexName          Name of the index associated with the connector.
     * @param isNative           Flag indicating whether the connector is a native type.
     * @param language           The language supported by the connector.
     * @param syncInfo           Information about the synchronization state of the connector.
     * @param name               Name of the connector.
     * @param pipeline           Ingest pipeline configuration.
     * @param scheduling         Scheduling settings for regular data synchronization.
     * @param serviceType        Type of service the connector integrates with.
     * @param status             Current status of the connector.
     * @param syncCursor         Position or state indicating the current point of synchronization.
     * @param syncNow            Flag indicating whether an immediate synchronization is requested.
     */
    private Connector(
        String connectorId,
        String apiKeyId,
        Map<String, Object> configuration,
        ConnectorCustomSchedule customScheduling,
        String description,
        String error,
        ConnectorFeatures features,
        List<ConnectorFiltering> filtering,
        String indexName,
        boolean isNative,
        String language,
        ConnectorSyncInfo syncInfo,
        String name,
        ConnectorIngestPipeline pipeline,
        ConnectorScheduling scheduling,
        String serviceType,
        ConnectorStatus status,
        Object syncCursor,
        boolean syncNow
    ) {
        this.connectorId = Objects.requireNonNull(connectorId, "connectorId cannot be null");
        this.apiKeyId = apiKeyId;
        this.configuration = configuration;
        this.customScheduling = customScheduling;
        this.description = description;
        this.error = error;
        this.features = features;
        this.filtering = filtering;
        this.indexName = indexName;
        this.isNative = isNative;
        this.language = language;
        this.syncInfo = syncInfo;
        this.name = name;
        this.pipeline = pipeline;
        this.scheduling = scheduling;
        this.serviceType = serviceType;
        this.status = Objects.requireNonNull(status, "connector status cannot be null");
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
        this.filtering = in.readOptionalCollectionAsList(ConnectorFiltering::new);
        this.indexName = in.readOptionalString();
        this.isNative = in.readBoolean();
        this.language = in.readOptionalString();
        this.syncInfo = in.readOptionalWriteable(ConnectorSyncInfo::new);
        this.name = in.readOptionalString();
        this.pipeline = in.readOptionalWriteable(ConnectorIngestPipeline::new);
        this.scheduling = in.readOptionalWriteable(ConnectorScheduling::new);
        this.serviceType = in.readOptionalString();
        this.status = in.readEnum(ConnectorStatus.class);
        this.syncCursor = in.readGenericValue();
        this.syncNow = in.readBoolean();
    }

    private static final ParseField ID_FIELD = new ParseField("connector_id");
    private static final ParseField API_KEY_ID_FIELD = new ParseField("api_key_id");
    private static final ParseField CONFIGURATION_FIELD = new ParseField("configuration");
    private static final ParseField CUSTOM_SCHEDULING_FIELD = new ParseField("custom_scheduling");
    private static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    private static final ParseField ERROR_FIELD = new ParseField("error");
    private static final ParseField FEATURES_FIELD = new ParseField("features");
    private static final ParseField FILTERING_FIELD = new ParseField("filtering");
    private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    private static final ParseField IS_NATIVE_FIELD = new ParseField("is_native");
    private static final ParseField LANGUAGE_FIELD = new ParseField("language");

    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField PIPELINE_FIELD = new ParseField("pipeline");
    private static final ParseField SCHEDULING_FIELD = new ParseField("scheduling");
    private static final ParseField SERVICE_TYPE_FIELD = new ParseField("service_type");
    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ParseField SYNC_CURSOR_FIELD = new ParseField("sync_cursor");
    private static final ParseField SYNC_NOW_FIELD = new ParseField("sync_now");

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
            if (filtering != null) {
                builder.xContentList(FILTERING_FIELD.getPreferredName(), filtering);
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
            builder.field(STATUS_FIELD.getPreferredName(), status.toString());
            builder.field(SYNC_NOW_FIELD.getPreferredName(), syncNow);

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
        out.writeOptionalCollection(filtering);
        out.writeOptionalString(indexName);
        out.writeBoolean(isNative);
        out.writeOptionalString(language);
        out.writeOptionalWriteable(syncInfo);
        out.writeOptionalString(name);
        out.writeOptionalWriteable(pipeline);
        out.writeOptionalWriteable(scheduling);
        out.writeOptionalString(serviceType);
        out.writeEnum(status);
        out.writeGenericValue(syncCursor);
        out.writeBoolean(syncNow);
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Connector connector = (Connector) o;
        return isNative == connector.isNative
            && syncNow == connector.syncNow
            && Objects.equals(connectorId, connector.connectorId)
            && Objects.equals(apiKeyId, connector.apiKeyId)
            && Objects.equals(configuration, connector.configuration)
            && Objects.equals(customScheduling, connector.customScheduling)
            && Objects.equals(description, connector.description)
            && Objects.equals(error, connector.error)
            && Objects.equals(features, connector.features)
            && Objects.equals(filtering, connector.filtering)
            && Objects.equals(indexName, connector.indexName)
            && Objects.equals(language, connector.language)
            && Objects.equals(syncInfo, connector.syncInfo)
            && Objects.equals(name, connector.name)
            && Objects.equals(pipeline, connector.pipeline)
            && Objects.equals(scheduling, connector.scheduling)
            && Objects.equals(serviceType, connector.serviceType)
            && status == connector.status
            && Objects.equals(syncCursor, connector.syncCursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            connectorId,
            apiKeyId,
            configuration,
            customScheduling,
            description,
            error,
            features,
            filtering,
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

    public static class Builder {

        private String connectorId;
        private String apiKeyId;
        private Map<String, Object> configuration = Collections.emptyMap();
        private ConnectorCustomSchedule customScheduling;
        private String description;
        private String error;
        private ConnectorFeatures features;
        private List<ConnectorFiltering> filtering = List.of(ConnectorFiltering.getDefaultConnectorFilteringConfig());
        private String indexName;
        private boolean isNative = false;
        private String language;
        private ConnectorSyncInfo syncInfo = new ConnectorSyncInfo.Builder().build();
        private String name;
        private ConnectorIngestPipeline pipeline;
        private ConnectorScheduling scheduling = ConnectorScheduling.getDefaultConnectorScheduling();
        private String serviceType;
        private ConnectorStatus status = ConnectorStatus.CREATED;
        private Object syncCursor;
        private boolean syncNow = false;

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

        public Builder setFeatures(ConnectorFeatures features) {
            this.features = features;
            return this;
        }

        public Builder setFiltering(List<ConnectorFiltering> filtering) {
            this.filtering = filtering;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setIsNative(boolean isNative) {
            this.isNative = isNative;
            if (isNative) {
                this.status = ConnectorStatus.NEEDS_CONFIGURATION;
            }
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
            this.name = Objects.requireNonNullElse(name, "");
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

        public Builder setSyncNow(boolean syncNow) {
            this.syncNow = syncNow;
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
                filtering,
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
