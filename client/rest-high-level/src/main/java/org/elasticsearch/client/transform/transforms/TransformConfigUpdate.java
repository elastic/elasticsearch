/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the mutable configuration items for a transform
 */
public class TransformConfigUpdate implements ToXContentObject {

    public static final String NAME = "transform_config_update";
    private static final ConstructingObjectParser<TransformConfigUpdate, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args) -> {
            SourceConfig source = (SourceConfig) args[0];
            DestConfig dest = (DestConfig) args[1];
            TimeValue frequency = args[2] == null
                ? null
                : TimeValue.parseTimeValue((String) args[2], TransformConfig.FREQUENCY.getPreferredName());
            SyncConfig syncConfig = (SyncConfig) args[3];
            String description = (String) args[4];
            SettingsConfig settings = (SettingsConfig) args[5];
            RetentionPolicyConfig retentionPolicyConfig = (RetentionPolicyConfig) args[6];
            return new TransformConfigUpdate(source, dest, frequency, syncConfig, description, settings, retentionPolicyConfig);
        }
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SourceConfig.PARSER.apply(p, null), TransformConfig.SOURCE);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> DestConfig.PARSER.apply(p, null), TransformConfig.DEST);
        PARSER.declareString(optionalConstructorArg(), TransformConfig.FREQUENCY);
        PARSER.declareNamedObject(optionalConstructorArg(), (p, c, n) -> p.namedObject(SyncConfig.class, n, c), TransformConfig.SYNC);
        PARSER.declareString(optionalConstructorArg(), TransformConfig.DESCRIPTION);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SettingsConfig.fromXContent(p), TransformConfig.SETTINGS);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(RetentionPolicyConfig.class, n, c),
            TransformConfig.RETENTION_POLICY
        );
    }

    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final String description;
    private final SettingsConfig settings;

    public TransformConfigUpdate(
        final SourceConfig source,
        final DestConfig dest,
        final TimeValue frequency,
        final SyncConfig syncConfig,
        final String description,
        final SettingsConfig settings,
        final RetentionPolicyConfig retentionPolicyConfig
    ) {
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.description = description;
        this.settings = settings;
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

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (source != null) {
            builder.field(TransformConfig.SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(TransformConfig.DEST.getPreferredName(), dest);
        }
        if (frequency != null) {
            builder.field(TransformConfig.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(TransformConfig.SYNC.getPreferredName());
            builder.field(syncConfig.getName(), syncConfig);
            builder.endObject();
        }
        if (description != null) {
            builder.field(TransformConfig.DESCRIPTION.getPreferredName(), description);
        }
        if (settings != null) {
            builder.field(TransformConfig.SETTINGS.getPreferredName(), settings);
        }

        builder.endObject();
        return builder;
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
            && Objects.equals(this.settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, dest, frequency, syncConfig, description, settings);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TransformConfigUpdate fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class Builder {

        private SourceConfig source;
        private DestConfig dest;
        private TimeValue frequency;
        private SyncConfig syncConfig;
        private String description;
        private SettingsConfig settings;
        private RetentionPolicyConfig retentionPolicyConfig;

        public Builder setSource(SourceConfig source) {
            this.source = source;
            return this;
        }

        public Builder setDest(DestConfig dest) {
            this.dest = dest;
            return this;
        }

        public Builder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        public Builder setSyncConfig(SyncConfig syncConfig) {
            this.syncConfig = syncConfig;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setSettings(SettingsConfig settings) {
            this.settings = settings;
            return this;
        }

        public Builder setRetentionPolicyConfig(RetentionPolicyConfig retentionPolicyConfig) {
            this.retentionPolicyConfig = retentionPolicyConfig;
            return this;
        }

        public TransformConfigUpdate build() {
            return new TransformConfigUpdate(source, dest, frequency, syncConfig, description, settings, retentionPolicyConfig);
        }
    }
}
