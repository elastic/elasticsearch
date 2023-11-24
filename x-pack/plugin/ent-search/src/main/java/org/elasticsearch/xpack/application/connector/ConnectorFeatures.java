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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ConnectorFeatures implements Writeable, ToXContentObject {

    @Nullable
    private final Boolean documentLevelSecurityEnabled;
    @Nullable
    private final Boolean filteringAdvancedConfigEnabled;
    @Nullable
    private final Boolean filteringRulesEnabled;
    @Nullable
    private final Boolean incrementalSyncEnabled;
    @Nullable
    private final Boolean syncRulesAdvancedEnabled;
    @Nullable
    private final Boolean syncRulesBasicEnabled;

    /**
     * Constructs a new instance of ConnectorFeatures.
     *
     * @param documentLevelSecurityEnabled A flag indicating whether document-level security is enabled.
     * @param filteringAdvancedConfig      A flag indicating whether advanced filtering configuration is enabled.
     * @param filteringRules               A flag indicating whether filtering rules are enabled.
     * @param incrementalSyncEnabled       A flag indicating whether incremental synchronization is enabled.
     * @param syncRulesAdvancedEnabled     A flag indicating whether advanced synchronization rules are enabled.
     * @param syncRulesBasicEnabled        A flag indicating whether basic synchronization rules are enabled.
     */
    private ConnectorFeatures(
        Boolean documentLevelSecurityEnabled,
        Boolean filteringAdvancedConfig,
        Boolean filteringRules,
        Boolean incrementalSyncEnabled,
        Boolean syncRulesAdvancedEnabled,
        Boolean syncRulesBasicEnabled
    ) {
        this.documentLevelSecurityEnabled = documentLevelSecurityEnabled;
        this.filteringAdvancedConfigEnabled = filteringAdvancedConfig;
        this.filteringRulesEnabled = filteringRules;
        this.incrementalSyncEnabled = incrementalSyncEnabled;
        this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
        this.syncRulesBasicEnabled = syncRulesBasicEnabled;
    }

    public ConnectorFeatures(StreamInput in) throws IOException {
        this.documentLevelSecurityEnabled = in.readOptionalBoolean();
        this.filteringAdvancedConfigEnabled = in.readOptionalBoolean();
        this.filteringRulesEnabled = in.readOptionalBoolean();
        this.incrementalSyncEnabled = in.readOptionalBoolean();
        this.syncRulesAdvancedEnabled = in.readOptionalBoolean();
        this.syncRulesBasicEnabled = in.readOptionalBoolean();
    }

    private static final ParseField DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD = new ParseField("document_level_security");
    private static final ParseField FILTERING_ADVANCED_CONFIG_ENABLED_FIELD = new ParseField("filtering_advanced_config");
    private static final ParseField FILTERING_RULES_ENABLED_FIELD = new ParseField("filtering_rules");
    private static final ParseField INCREMENTAL_SYNC_ENABLED_FIELD = new ParseField("incremental_sync");
    private static final ParseField SYNC_RULES_ADVANCED_ENABLED_FIELD = new ParseField("advanced_sync_rules");
    private static final ParseField SYNC_RULES_BASIC_ENABLED_FIELD = new ParseField("basic_sync_rules");

    private static final ConstructingObjectParser<ConnectorFeatures, Void> PARSER = new ConstructingObjectParser<>(
        "connector_features",
        true,
        args -> new Builder().setDocumentLevelSecurityEnabled((Boolean) args[0])
            .setFilteringAdvancedConfig((Boolean) args[1])
            .setFilteringRules((Boolean) args[2])
            .setIncrementalSyncEnabled((Boolean) args[3])
            .setSyncRulesAdvancedEnabled((Boolean) args[4])
            .setSyncRulesBasicEnabled((Boolean) args[5])
            .build()
    );

    static {
        PARSER.declareBoolean(optionalConstructorArg(), DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), FILTERING_ADVANCED_CONFIG_ENABLED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), FILTERING_RULES_ENABLED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), INCREMENTAL_SYNC_ENABLED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), SYNC_RULES_ADVANCED_ENABLED_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), SYNC_RULES_BASIC_ENABLED_FIELD);
    }

    public static ConnectorFeatures fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (documentLevelSecurityEnabled != null) {
                builder.startObject(DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD.getPreferredName());
                {
                    builder.field("enabled", documentLevelSecurityEnabled);
                }
                builder.endObject();
            }
            if (filteringAdvancedConfigEnabled != null) {
                builder.field(FILTERING_ADVANCED_CONFIG_ENABLED_FIELD.getPreferredName(), filteringAdvancedConfigEnabled);
            }
            if (filteringRulesEnabled != null) {
                builder.field(FILTERING_RULES_ENABLED_FIELD.getPreferredName(), filteringRulesEnabled);
            }
            if (incrementalSyncEnabled != null) {
                builder.startObject(INCREMENTAL_SYNC_ENABLED_FIELD.getPreferredName());
                {
                    builder.field("enabled", incrementalSyncEnabled);
                }
                builder.endObject();
            }
            builder.startObject("sync_rules");
            {
                if (syncRulesAdvancedEnabled != null) {
                    builder.startObject("advanced");
                    {
                        builder.field("enabled", syncRulesAdvancedEnabled);
                    }
                    builder.endObject();
                }
                if (syncRulesBasicEnabled != null) {
                    builder.startObject("basic");
                    {
                        builder.field("enabled", syncRulesBasicEnabled);
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(documentLevelSecurityEnabled);
        out.writeOptionalBoolean(filteringAdvancedConfigEnabled);
        out.writeOptionalBoolean(filteringRulesEnabled);
        out.writeOptionalBoolean(incrementalSyncEnabled);
        out.writeOptionalBoolean(syncRulesAdvancedEnabled);
        out.writeOptionalBoolean(syncRulesBasicEnabled);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorFeatures that = (ConnectorFeatures) o;
        return Objects.equals(documentLevelSecurityEnabled, that.documentLevelSecurityEnabled)
            && Objects.equals(filteringAdvancedConfigEnabled, that.filteringAdvancedConfigEnabled)
            && Objects.equals(filteringRulesEnabled, that.filteringRulesEnabled)
            && Objects.equals(incrementalSyncEnabled, that.incrementalSyncEnabled)
            && Objects.equals(syncRulesAdvancedEnabled, that.syncRulesAdvancedEnabled)
            && Objects.equals(syncRulesBasicEnabled, that.syncRulesBasicEnabled);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            documentLevelSecurityEnabled,
            filteringAdvancedConfigEnabled,
            filteringRulesEnabled,
            incrementalSyncEnabled,
            syncRulesAdvancedEnabled,
            syncRulesBasicEnabled
        );
    }

    public static class Builder {

        private Boolean documentLevelSecurityEnabled;
        private Boolean filteringAdvancedConfig;
        private Boolean filteringRules;
        private Boolean incrementalSyncEnabled;
        private Boolean syncRulesAdvancedEnabled;
        private Boolean syncRulesBasicEnabled;

        public Builder setDocumentLevelSecurityEnabled(Boolean documentLevelSecurityEnabled) {
            this.documentLevelSecurityEnabled = documentLevelSecurityEnabled;
            return this;
        }

        public Builder setFilteringAdvancedConfig(Boolean filteringAdvancedConfig) {
            this.filteringAdvancedConfig = filteringAdvancedConfig;
            return this;
        }

        public Builder setFilteringRules(Boolean filteringRules) {
            this.filteringRules = filteringRules;
            return this;
        }

        public Builder setIncrementalSyncEnabled(Boolean incrementalSyncEnabled) {
            this.incrementalSyncEnabled = incrementalSyncEnabled;
            return this;
        }

        public Builder setSyncRulesAdvancedEnabled(Boolean syncRulesAdvancedEnabled) {
            this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
            return this;
        }

        public Builder setSyncRulesBasicEnabled(Boolean syncRulesBasicEnabled) {
            this.syncRulesBasicEnabled = syncRulesBasicEnabled;
            return this;
        }

        public ConnectorFeatures build() {
            return new ConnectorFeatures(
                documentLevelSecurityEnabled,
                filteringAdvancedConfig,
                filteringRules,
                incrementalSyncEnabled,
                syncRulesAdvancedEnabled,
                syncRulesBasicEnabled
            );
        }
    }
}
