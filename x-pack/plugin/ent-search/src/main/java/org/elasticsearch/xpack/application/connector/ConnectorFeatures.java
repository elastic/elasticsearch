/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The {@link ConnectorFeatures} class represents feature flags for a connector.
 */
public class ConnectorFeatures implements Writeable, ToXContentObject {

    @Nullable
    private final FeatureEnabled documentLevelSecurityEnabled;
    @Nullable
    private final FeatureEnabled incrementalSyncEnabled;
    @Nullable
    private final FeatureEnabled nativeConnectorAPIKeysEnabled;
    @Nullable
    private final SyncRulesFeatures syncRulesFeatures;

    /**
     * Constructs a new instance of ConnectorFeatures.
     *
     * @param documentLevelSecurityEnabled  A flag indicating whether document-level security is enabled.
     * @param incrementalSyncEnabled        A flag indicating whether incremental sync is enabled.
     * @param nativeConnectorAPIKeysEnabled A flag indicating whether support for api keys is enabled for native connectors.
     * @param syncRulesFeatures             An {@link SyncRulesFeatures} object indicating if basic and advanced sync rules are enabled.
     */
    private ConnectorFeatures(
        FeatureEnabled documentLevelSecurityEnabled,
        FeatureEnabled incrementalSyncEnabled,
        FeatureEnabled nativeConnectorAPIKeysEnabled,
        SyncRulesFeatures syncRulesFeatures
    ) {
        this.documentLevelSecurityEnabled = documentLevelSecurityEnabled;
        this.incrementalSyncEnabled = incrementalSyncEnabled;
        this.nativeConnectorAPIKeysEnabled = nativeConnectorAPIKeysEnabled;
        this.syncRulesFeatures = syncRulesFeatures;
    }

    public ConnectorFeatures(StreamInput in) throws IOException {
        this.documentLevelSecurityEnabled = in.readOptionalWriteable(FeatureEnabled::new);
        this.incrementalSyncEnabled = in.readOptionalWriteable(FeatureEnabled::new);
        this.nativeConnectorAPIKeysEnabled = in.readOptionalWriteable(FeatureEnabled::new);
        this.syncRulesFeatures = in.readOptionalWriteable(SyncRulesFeatures::new);
    }

    private static final ParseField DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD = new ParseField("document_level_security");
    private static final ParseField INCREMENTAL_SYNC_ENABLED_FIELD = new ParseField("incremental_sync");
    private static final ParseField NATIVE_CONNECTOR_API_KEYS_ENABLED_FIELD = new ParseField("native_connector_api_keys");
    private static final ParseField SYNC_RULES_FIELD = new ParseField("sync_rules");

    private static final ConstructingObjectParser<ConnectorFeatures, Void> PARSER = new ConstructingObjectParser<>(
        "connector_features",
        true,
        args -> new Builder().setDocumentLevelSecurityEnabled((FeatureEnabled) args[0])
            .setIncrementalSyncEnabled((FeatureEnabled) args[1])
            .setNativeConnectorAPIKeysEnabled((FeatureEnabled) args[2])
            .setSyncRulesFeatures((SyncRulesFeatures) args[3])
            .build()
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> FeatureEnabled.fromXContent(p), DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> FeatureEnabled.fromXContent(p), INCREMENTAL_SYNC_ENABLED_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> FeatureEnabled.fromXContent(p), NATIVE_CONNECTOR_API_KEYS_ENABLED_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SyncRulesFeatures.fromXContent(p), SYNC_RULES_FIELD);
    }

    public static ConnectorFeatures fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ConnectorFeatures fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ConnectorFeatures.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse a connector features.", e);
        }
    }

    public FeatureEnabled getDocumentLevelSecurityEnabled() {
        return documentLevelSecurityEnabled;
    }

    public FeatureEnabled getIncrementalSyncEnabled() {
        return incrementalSyncEnabled;
    }

    public FeatureEnabled getNativeConnectorAPIKeysEnabled() {
        return nativeConnectorAPIKeysEnabled;
    }

    public SyncRulesFeatures getSyncRulesFeatures() {
        return syncRulesFeatures;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (documentLevelSecurityEnabled != null) {
                builder.field(DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD.getPreferredName(), documentLevelSecurityEnabled);
            }
            if (incrementalSyncEnabled != null) {
                builder.field(INCREMENTAL_SYNC_ENABLED_FIELD.getPreferredName(), incrementalSyncEnabled);
            }
            if (nativeConnectorAPIKeysEnabled != null) {
                builder.field(NATIVE_CONNECTOR_API_KEYS_ENABLED_FIELD.getPreferredName(), nativeConnectorAPIKeysEnabled);
            }
            if (syncRulesFeatures != null) {
                builder.field(SYNC_RULES_FIELD.getPreferredName(), syncRulesFeatures);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(documentLevelSecurityEnabled);
        out.writeOptionalWriteable(incrementalSyncEnabled);
        out.writeOptionalWriteable(nativeConnectorAPIKeysEnabled);
        out.writeOptionalWriteable(syncRulesFeatures);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorFeatures features = (ConnectorFeatures) o;
        return Objects.equals(documentLevelSecurityEnabled, features.documentLevelSecurityEnabled)
            && Objects.equals(incrementalSyncEnabled, features.incrementalSyncEnabled)
            && Objects.equals(nativeConnectorAPIKeysEnabled, features.nativeConnectorAPIKeysEnabled)
            && Objects.equals(syncRulesFeatures, features.syncRulesFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentLevelSecurityEnabled, incrementalSyncEnabled, nativeConnectorAPIKeysEnabled, syncRulesFeatures);
    }

    public static class Builder {

        private FeatureEnabled documentLevelSecurityEnabled;
        private FeatureEnabled incrementalSyncEnabled;
        private FeatureEnabled nativeConnectorAPIKeysEnabled;
        private SyncRulesFeatures syncRulesFeatures;

        public Builder setDocumentLevelSecurityEnabled(FeatureEnabled documentLevelSecurityEnabled) {
            this.documentLevelSecurityEnabled = documentLevelSecurityEnabled;
            return this;
        }

        public Builder setIncrementalSyncEnabled(FeatureEnabled incrementalSyncEnabled) {
            this.incrementalSyncEnabled = incrementalSyncEnabled;
            return this;
        }

        public Builder setNativeConnectorAPIKeysEnabled(FeatureEnabled nativeConnectorAPIKeysEnabled) {
            this.nativeConnectorAPIKeysEnabled = nativeConnectorAPIKeysEnabled;
            return this;
        }

        public Builder setSyncRulesFeatures(SyncRulesFeatures syncRulesFeatures) {
            this.syncRulesFeatures = syncRulesFeatures;
            return this;
        }

        public ConnectorFeatures build() {
            return new ConnectorFeatures(
                documentLevelSecurityEnabled,
                incrementalSyncEnabled,
                nativeConnectorAPIKeysEnabled,
                syncRulesFeatures
            );
        }
    }

    /**
     * The {@link FeatureEnabled} class serves as a helper for serializing and deserializing
     * feature representations within the Connector context. This class specifically addresses
     * the handling of features represented in a nested JSON structure:
     *
     * <pre>
     *     "my_feature": {"enabled": true}
     * </pre>
     */
    public static class FeatureEnabled implements ToXContentObject, Writeable {

        private final boolean enabled;

        public FeatureEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public FeatureEnabled(StreamInput in) throws IOException {
            this.enabled = in.readBoolean();
        }

        private static final ParseField ENABLED_FIELD = new ParseField("enabled");

        private static final ConstructingObjectParser<FeatureEnabled, Void> PARSER = new ConstructingObjectParser<>(
            "connector_feature_enabled",
            true,
            args -> new FeatureEnabled((boolean) args[0])
        );

        static {
            PARSER.declareBoolean(optionalConstructorArg(), ENABLED_FIELD);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ENABLED_FIELD.getPreferredName(), enabled);
            }
            builder.endObject();
            return builder;
        }

        public static FeatureEnabled fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FeatureEnabled that = (FeatureEnabled) o;
            return enabled == that.enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled);
        }
    }

    /**
     * The {@link SyncRulesFeatures} class represents the feature configuration for advanced and basic
     * sync rules in a structured and serializable format.
     */
    public static class SyncRulesFeatures implements ToXContentObject, Writeable {

        private final FeatureEnabled syncRulesAdvancedEnabled;
        private final FeatureEnabled syncRulesBasicEnabled;

        private SyncRulesFeatures(FeatureEnabled syncRulesAdvancedEnabled, FeatureEnabled syncRulesBasicEnabled) {
            this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
            this.syncRulesBasicEnabled = syncRulesBasicEnabled;
        }

        public SyncRulesFeatures(StreamInput in) throws IOException {
            this.syncRulesAdvancedEnabled = in.readOptionalWriteable(FeatureEnabled::new);
            this.syncRulesBasicEnabled = in.readOptionalWriteable(FeatureEnabled::new);
        }

        private static final ParseField SYNC_RULES_ADVANCED_ENABLED_FIELD = new ParseField("advanced");
        private static final ParseField SYNC_RULES_BASIC_ENABLED_FIELD = new ParseField("basic");

        private static final ConstructingObjectParser<SyncRulesFeatures, Void> PARSER = new ConstructingObjectParser<>(
            "sync_rules_features",
            true,
            args -> new Builder().setSyncRulesAdvancedEnabled((FeatureEnabled) args[0])
                .setSyncRulesBasicEnabled((FeatureEnabled) args[1])
                .build()
        );

        static {
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> FeatureEnabled.fromXContent(p), SYNC_RULES_ADVANCED_ENABLED_FIELD);
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> FeatureEnabled.fromXContent(p), SYNC_RULES_BASIC_ENABLED_FIELD);
        }

        public static SyncRulesFeatures fromXContent(XContentParser p) throws IOException {
            return PARSER.parse(p, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                if (syncRulesAdvancedEnabled != null) {
                    builder.field(SYNC_RULES_ADVANCED_ENABLED_FIELD.getPreferredName(), syncRulesAdvancedEnabled);
                }
                if (syncRulesBasicEnabled != null) {
                    builder.field(SYNC_RULES_BASIC_ENABLED_FIELD.getPreferredName(), syncRulesBasicEnabled);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(syncRulesAdvancedEnabled);
            out.writeOptionalWriteable(syncRulesBasicEnabled);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SyncRulesFeatures that = (SyncRulesFeatures) o;
            return Objects.equals(syncRulesAdvancedEnabled, that.syncRulesAdvancedEnabled)
                && Objects.equals(syncRulesBasicEnabled, that.syncRulesBasicEnabled);
        }

        @Override
        public int hashCode() {
            return Objects.hash(syncRulesAdvancedEnabled, syncRulesBasicEnabled);
        }

        public static class Builder {

            private FeatureEnabled syncRulesAdvancedEnabled;
            private FeatureEnabled syncRulesBasicEnabled;

            public Builder setSyncRulesAdvancedEnabled(FeatureEnabled syncRulesAdvancedEnabled) {
                this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
                return this;
            }

            public Builder setSyncRulesBasicEnabled(FeatureEnabled syncRulesBasicEnabled) {
                this.syncRulesBasicEnabled = syncRulesBasicEnabled;
                return this;
            }

            public SyncRulesFeatures build() {
                return new SyncRulesFeatures(syncRulesAdvancedEnabled, syncRulesBasicEnabled);
            }
        }
    }
}
