/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.transform.transforms.latest.LatestConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformConfig implements ToXContentObject {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField SYNC = new ParseField("sync");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField RETENTION_POLICY = new ParseField("retention_policy");
    // types of transforms
    public static final ParseField PIVOT_TRANSFORM = new ParseField("pivot");
    public static final ParseField LATEST_TRANSFORM = new ParseField("latest");

    private final String id;
    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final SettingsConfig settings;
    private final PivotConfig pivotConfig;
    private final LatestConfig latestConfig;
    private final String description;
    private final RetentionPolicyConfig retentionPolicyConfig;
    private final Version transformVersion;
    private final Instant createTime;

    public static final ConstructingObjectParser<TransformConfig, Void> PARSER = new ConstructingObjectParser<>(
        "transform",
        true,
        (args) -> {
            String id = (String) args[0];
            SourceConfig source = (SourceConfig) args[1];
            DestConfig dest = (DestConfig) args[2];
            TimeValue frequency = (TimeValue) args[3];
            SyncConfig syncConfig = (SyncConfig) args[4];
            PivotConfig pivotConfig = (PivotConfig) args[5];
            LatestConfig latestConfig = (LatestConfig) args[6];
            String description = (String) args[7];
            SettingsConfig settings = (SettingsConfig) args[8];
            RetentionPolicyConfig retentionPolicyConfig = (RetentionPolicyConfig) args[9];
            Instant createTime = (Instant) args[10];
            String transformVersion = (String) args[11];
            return new TransformConfig(
                id,
                source,
                dest,
                frequency,
                syncConfig,
                pivotConfig,
                latestConfig,
                description,
                settings,
                retentionPolicyConfig,
                createTime,
                transformVersion
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), ID);
        PARSER.declareObject(constructorArg(), (p, c) -> SourceConfig.PARSER.apply(p, null), SOURCE);
        PARSER.declareObject(constructorArg(), (p, c) -> DestConfig.PARSER.apply(p, null), DEST);
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeValue.parseTimeValue(p.text(), FREQUENCY.getPreferredName()),
            FREQUENCY,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareNamedObject(optionalConstructorArg(), (p, c, n) -> p.namedObject(SyncConfig.class, n, c), SYNC);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> PivotConfig.fromXContent(p), PIVOT_TRANSFORM);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> LatestConfig.fromXContent(p), LATEST_TRANSFORM);
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SettingsConfig.fromXContent(p), SETTINGS);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(RetentionPolicyConfig.class, n, c),
            RETENTION_POLICY
        );
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareString(optionalConstructorArg(), VERSION);
    }

    public static TransformConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Helper method for previewing a transform configuration
     *
     * The TransformConfig returned from this method should only be used for previewing the resulting data.
     *
     * A new, valid, TransformConfig with an appropriate destination and ID will have to be constructed to create
     * the transform.
     * @param source Source configuration for gathering the data
     * @param pivotConfig Config to preview
     * @return A TransformConfig to preview, NOTE it will have a {@code null} id, destination and index.
     */
    public static TransformConfig forPreview(final SourceConfig source, final PivotConfig pivotConfig) {
        return new TransformConfig(null, source, null, null, null, pivotConfig, null, null, null, null, null, null);
    }

    /**
     * Helper method for previewing a transform configuration
     *
     * The TransformConfig returned from this method should only be used for previewing the resulting data.
     *
     * A new, valid, TransformConfig with an appropriate destination and ID will have to be constructed to create
     * the transform.
     * @param source Source configuration for gathering the data
     * @param latestConfig Config to preview
     * @return A TransformConfig to preview, NOTE it will have a {@code null} id, destination and index.
     */
    public static TransformConfig forPreview(final SourceConfig source, final LatestConfig latestConfig) {
        return new TransformConfig(null, source, null, null, null, null, latestConfig, null, null, null, null, null);
    }

    TransformConfig(
        final String id,
        final SourceConfig source,
        final DestConfig dest,
        final TimeValue frequency,
        final SyncConfig syncConfig,
        final PivotConfig pivotConfig,
        final LatestConfig latestConfig,
        final String description,
        final SettingsConfig settings,
        final RetentionPolicyConfig retentionPolicyConfig,
        final Instant createTime,
        final String version
    ) {
        this.id = id;
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.pivotConfig = pivotConfig;
        this.latestConfig = latestConfig;
        this.description = description;
        this.settings = settings;
        this.retentionPolicyConfig = retentionPolicyConfig;
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.transformVersion = version == null ? null : Version.fromString(version);
    }

    public String getId() {
        return id;
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

    public PivotConfig getPivotConfig() {
        return pivotConfig;
    }

    public LatestConfig getLatestConfig() {
        return latestConfig;
    }

    public Version getVersion() {
        return transformVersion;
    }

    public Instant getCreateTime() {
        return createTime;
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
    public RetentionPolicyConfig getRetentionPolicyConfig() {
        return retentionPolicyConfig;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field(ID.getPreferredName(), id);
        }
        if (source != null) {
            builder.field(SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(DEST.getPreferredName(), dest);
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(SYNC.getPreferredName());
            builder.field(syncConfig.getName(), syncConfig);
            builder.endObject();
        }
        if (pivotConfig != null) {
            builder.field(PIVOT_TRANSFORM.getPreferredName(), pivotConfig);
        }
        if (latestConfig != null) {
            builder.field(LATEST_TRANSFORM.getPreferredName(), latestConfig);
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (settings != null) {
            builder.field(SETTINGS.getPreferredName(), settings);
        }
        if (retentionPolicyConfig != null) {
            builder.startObject(RETENTION_POLICY.getPreferredName());
            builder.field(retentionPolicyConfig.getName(), retentionPolicyConfig);
            builder.endObject();
        }
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (transformVersion != null) {
            builder.field(VERSION.getPreferredName(), transformVersion);
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

        final TransformConfig that = (TransformConfig) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.source, that.source)
            && Objects.equals(this.dest, that.dest)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.syncConfig, that.syncConfig)
            && Objects.equals(this.transformVersion, that.transformVersion)
            && Objects.equals(this.settings, that.settings)
            && Objects.equals(this.createTime, that.createTime)
            && Objects.equals(this.pivotConfig, that.pivotConfig)
            && Objects.equals(this.latestConfig, that.latestConfig)
            && Objects.equals(this.retentionPolicyConfig, that.retentionPolicyConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            source,
            dest,
            frequency,
            syncConfig,
            settings,
            createTime,
            transformVersion,
            pivotConfig,
            latestConfig,
            description,
            retentionPolicyConfig
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private SourceConfig source;
        private DestConfig dest;
        private TimeValue frequency;
        private SyncConfig syncConfig;
        private PivotConfig pivotConfig;
        private LatestConfig latestConfig;
        private SettingsConfig settings;
        private String description;
        private RetentionPolicyConfig retentionPolicyConfig;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

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

        public Builder setPivotConfig(PivotConfig pivotConfig) {
            this.pivotConfig = pivotConfig;
            return this;
        }

        public Builder setLatestConfig(LatestConfig latestConfig) {
            this.latestConfig = latestConfig;
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

        public TransformConfig build() {
            return new TransformConfig(
                id,
                source,
                dest,
                frequency,
                syncConfig,
                pivotConfig,
                latestConfig,
                description,
                settings,
                retentionPolicyConfig,
                null,
                null
            );
        }
    }
}
