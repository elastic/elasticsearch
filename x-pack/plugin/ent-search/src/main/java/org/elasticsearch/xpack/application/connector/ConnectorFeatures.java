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

    public static final ParseField DOCUMENT_LEVEL_SECURITY_ENABLED_FIELD = new ParseField("document_level_security_enabled");
    public static final ParseField FILTERING_ADVANCED_CONFIG_ENABLED_FIELD = new ParseField("filtering_advanced_config_enabled");
    public static final ParseField FILTERING_RULES_ENABLED_FIELD = new ParseField("filtering_rules_enabled");
    public static final ParseField INCREMENTAL_SYNC_ENABLED_FIELD = new ParseField("incremental_sync_enabled");
    public static final ParseField SYNC_RULES_ADVANCED_ENABLED_FIELD = new ParseField("sync_rules_advanced_enabled");
    public static final ParseField SYNC_RULES_BASIC_ENABLED_FIELD = new ParseField("sync_rules_basic_enabled");

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
            builder.startObject("document_level_security");
            {
                builder.field("enabled", documentLevelSecurityEnabled);
            }
            builder.endObject();
            builder.field("filtering_advanced_config", filteringAdvancedConfigEnabled);
            builder.field("filtering_rules", filteringRulesEnabled);
            builder.startObject("incremental_sync");
            {
                builder.field("enabled", incrementalSyncEnabled);
            }
            builder.endObject();
            builder.startObject("sync_rules");
            {
                builder.startObject("advanced");
                {
                    builder.field("enabled", syncRulesAdvancedEnabled);
                }
                builder.endObject();
                builder.startObject("basic");
                {
                    builder.field("enabled", syncRulesBasicEnabled);
                }
                builder.endObject();
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
