/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.MAX_DESCRIPTION_LENGTH;

/**
 * This class holds the mutable configuration items for a data frame transform
 */
public class TransformConfigUpdate implements Writeable {

    public static final String NAME = "data_frame_transform_config_update";

    public static TransformConfigUpdate EMPTY = new TransformConfigUpdate(null, null, null, null, null, null, null, null);

    private static final ConstructingObjectParser<TransformConfigUpdate, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args) -> {
            SourceConfig source = (SourceConfig) args[0];
            DestConfig dest = (DestConfig) args[1];
            TimeValue frequency = args[2] == null
                ? null
                : TimeValue.parseTimeValue((String) args[2], TransformField.FREQUENCY.getPreferredName());
            SyncConfig syncConfig = (SyncConfig) args[3];
            String description = (String) args[4];
            SettingsConfig settings = (SettingsConfig) args[5];
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) args[6];
            RetentionPolicyConfig retentionPolicyConfig = (RetentionPolicyConfig) args[7];
            return new TransformConfigUpdate(source, dest, frequency, syncConfig, description, settings, metadata, retentionPolicyConfig);
        }
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SourceConfig.fromXContent(p, false), TransformField.SOURCE);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> DestConfig.fromXContent(p, false), TransformField.DESTINATION);
        PARSER.declareString(optionalConstructorArg(), TransformField.FREQUENCY);
        PARSER.declareNamedObject(optionalConstructorArg(), (p, c, n) -> p.namedObject(SyncConfig.class, n, c), TransformField.SYNC);
        PARSER.declareString(optionalConstructorArg(), TransformField.DESCRIPTION);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SettingsConfig.fromXContent(p, false), TransformField.SETTINGS);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), TransformField.METADATA);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> {
            XContentParser.Token token = p.nextToken();
            assert token == XContentParser.Token.FIELD_NAME;
            String currentName = p.currentName();
            RetentionPolicyConfig namedObject = p.namedObject(RetentionPolicyConfig.class, currentName, c);
            token = p.nextToken();
            assert token == XContentParser.Token.END_OBJECT;
            return namedObject;
        }, NullRetentionPolicyConfig.INSTANCE, TransformField.RETENTION_POLICY);
    }

    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final String description;
    private final SettingsConfig settings;
    private final Map<String, Object> metadata;
    private final RetentionPolicyConfig retentionPolicyConfig;
    private Map<String, String> headers;

    public TransformConfigUpdate(
        final SourceConfig source,
        final DestConfig dest,
        final TimeValue frequency,
        final SyncConfig syncConfig,
        final String description,
        final SettingsConfig settings,
        final Map<String, Object> metadata,
        final RetentionPolicyConfig retentionPolicyConfig
    ) {
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.description = description;
        if (this.description != null && this.description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("[description] must be less than 1000 characters in length.");
        }
        this.settings = settings;
        this.metadata = metadata;
        this.retentionPolicyConfig = retentionPolicyConfig;
    }

    public TransformConfigUpdate(final StreamInput in) throws IOException {
        source = in.readOptionalWriteable(SourceConfig::new);
        dest = in.readOptionalWriteable(DestConfig::new);
        frequency = in.readOptionalTimeValue();
        description = in.readOptionalString();
        syncConfig = in.readOptionalNamedWriteable(SyncConfig.class);
        if (in.readBoolean()) {
            setHeaders(in.readMap(StreamInput::readString, StreamInput::readString));
        }
        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            settings = in.readOptionalWriteable(SettingsConfig::new);
        } else {
            settings = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            metadata = in.readMap();
        } else {
            metadata = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_12_0)) {
            retentionPolicyConfig = in.readOptionalNamedWriteable(RetentionPolicyConfig.class);
        } else {
            retentionPolicyConfig = null;
        }
    }

    public SourceConfig getSource() {
        return source;
    }

    public DestConfig getDestination() {
        return dest;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Nullable
    public SettingsConfig getSettings() {
        return settings;
    }

    @Nullable
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Nullable
    public RetentionPolicyConfig getRetentionPolicyConfig() {
        return retentionPolicyConfig;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(source);
        out.writeOptionalWriteable(dest);
        out.writeOptionalTimeValue(frequency);
        out.writeOptionalString(description);
        out.writeOptionalNamedWriteable(syncConfig);
        if (headers != null) {
            out.writeBoolean(true);
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeOptionalWriteable(settings);
        }
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeGenericMap(metadata);
        }
        if (out.getVersion().onOrAfter(Version.V_7_12_0)) {
            out.writeOptionalNamedWriteable(retentionPolicyConfig);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TransformConfigUpdate that = (TransformConfigUpdate) other;

        return Objects.equals(this.source, that.source)
            && Objects.equals(this.dest, that.dest)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.syncConfig, that.syncConfig)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.settings, that.settings)
            && Objects.equals(this.metadata, that.metadata)
            && Objects.equals(this.retentionPolicyConfig, that.retentionPolicyConfig)
            && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, dest, frequency, syncConfig, description, settings, metadata, retentionPolicyConfig, headers);
    }

    public static TransformConfigUpdate fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public boolean isEmpty() {
        return this.equals(EMPTY);
    }

    boolean isNoop(TransformConfig config) {
        return isNullOrEqual(source, config.getSource())
            && isNullOrEqual(dest, config.getDestination())
            && isNullOrEqual(frequency, config.getFrequency())
            && isNullOrEqual(syncConfig, config.getSyncConfig())
            && isNullOrEqual(description, config.getDescription())
            && isNullOrEqual(settings, config.getSettings())
            && isNullOrEqual(metadata, config.getMetadata())
            && isNullOrEqual(retentionPolicyConfig, config.getRetentionPolicyConfig())
            && isNullOrEqual(headers, config.getHeaders());
    }

    public boolean changesSettings(TransformConfig config) {
        return isNullOrEqual(settings, config.getSettings()) == false;
    }

    private boolean isNullOrEqual(Object lft, Object rgt) {
        return lft == null || lft.equals(rgt);
    }

    public TransformConfig apply(TransformConfig config) {
        if (isNoop(config)) {
            return config;
        }
        TransformConfig.Builder builder = new TransformConfig.Builder(config);
        if (source != null) {
            builder.setSource(source);
        }
        if (dest != null) {
            builder.setDest(dest);
        }
        if (frequency != null) {
            builder.setFrequency(frequency);
        }
        if (syncConfig != null) {
            String currentConfigName = config.getSyncConfig() == null ? "null" : config.getSyncConfig().getWriteableName();
            if (syncConfig.getWriteableName().equals(currentConfigName) == false) {
                throw new ElasticsearchStatusException(
                    TransformMessages.getMessage(
                        TransformMessages.TRANSFORM_UPDATE_CANNOT_CHANGE_SYNC_METHOD,
                        config.getId(),
                        currentConfigName,
                        syncConfig.getWriteableName()
                    ),
                    RestStatus.BAD_REQUEST
                );
            }
            builder.setSyncConfig(syncConfig);
        }
        if (description != null) {
            builder.setDescription(description);
        }
        if (headers != null) {
            builder.setHeaders(headers);
        }
        if (settings != null) {
            // settings are partially updateable, that means we only overwrite changed settings but keep others
            SettingsConfig.Builder settingsBuilder = new SettingsConfig.Builder(config.getSettings());
            settingsBuilder.update(settings);
            builder.setSettings(settingsBuilder.build());
        }
        if (metadata != null) {
            // Unlike with settings, we fully replace the old metadata with the new metadata
            builder.setMetadata(metadata);
        }
        if (retentionPolicyConfig != null) {
            if (NullRetentionPolicyConfig.INSTANCE.equals(retentionPolicyConfig)) {
                builder.setRetentionPolicyConfig(null);
            } else {
                builder.setRetentionPolicyConfig(retentionPolicyConfig);
            }
        }

        builder.setVersion(Version.CURRENT);
        return builder.build();
    }
}
