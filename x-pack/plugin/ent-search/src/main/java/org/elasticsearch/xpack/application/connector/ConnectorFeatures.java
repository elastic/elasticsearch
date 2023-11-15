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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ConnectorFeatures implements Writeable, ToXContentObject {

    private final boolean filteringAdvancedConfigEnabled;
    private final boolean filteringRulesEnabled;
    private final boolean incrementalSyncEnabled;
    private final boolean syncRulesAdvancedEnabled;
    private final boolean syncRulesBasicEnabled;

    private ConnectorFeatures(
        boolean filteringAdvancedConfig,
        boolean filteringRules,
        boolean incrementalSyncEnabled,
        boolean syncRulesAdvancedEnabled,
        boolean syncRulesBasicEnabled
    ) {
        this.filteringAdvancedConfigEnabled = filteringAdvancedConfig;
        this.filteringRulesEnabled = filteringRules;
        this.incrementalSyncEnabled = incrementalSyncEnabled;
        this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
        this.syncRulesBasicEnabled = syncRulesBasicEnabled;
    }

    public ConnectorFeatures(StreamInput in) throws IOException {
        this.filteringAdvancedConfigEnabled = in.readBoolean();
        this.filteringRulesEnabled = in.readBoolean();
        this.incrementalSyncEnabled = in.readBoolean();
        this.syncRulesAdvancedEnabled = in.readBoolean();
        this.syncRulesBasicEnabled = in.readBoolean();
    }

    public static final ParseField FILTERING_ADVANCED_CONFIG_ENABLED_FIELD = new ParseField("filtering_advanced_config_enabled");
    public static final ParseField FILTERING_RULES_ENABLED_FIELD = new ParseField("filtering_rules_enabled");
    public static final ParseField INCREMENTAL_SYNC_ENABLED_FIELD = new ParseField("incremental_sync_enabled");
    public static final ParseField SYNC_RULES_ADVANCED_ENABLED_FIELD = new ParseField("sync_rules_advanced_enabled");
    public static final ParseField SYNC_RULES_BASIC_ENABLED_FIELD = new ParseField("sync_rules_basic_enabled");

    private static final ConstructingObjectParser<ConnectorFeatures, Void> PARSER = new ConstructingObjectParser<>(
        "connector_features",
        true,
        args -> new Builder().setFilteringAdvancedConfig((boolean) args[0])
            .setFilteringRules((boolean) args[1])
            .setIncrementalSyncEnabled((boolean) args[2])
            .setSyncRulesAdvancedEnabled((boolean) args[3])
            .setSyncRulesBasicEnabled((boolean) args[4])
            .createConnectorFeatures()
    );

    static {
        PARSER.declareBoolean(constructorArg(), FILTERING_ADVANCED_CONFIG_ENABLED_FIELD);
        PARSER.declareBoolean(constructorArg(), FILTERING_RULES_ENABLED_FIELD);
        PARSER.declareBoolean(constructorArg(), INCREMENTAL_SYNC_ENABLED_FIELD);
        PARSER.declareBoolean(constructorArg(), SYNC_RULES_ADVANCED_ENABLED_FIELD);
        PARSER.declareBoolean(constructorArg(), SYNC_RULES_BASIC_ENABLED_FIELD);
    }

    public static ConnectorFeatures fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject("properties");
            builder.field("filtering_advanced_config", filteringAdvancedConfigEnabled);
            builder.field("filtering_rules", filteringRulesEnabled);
            builder.startObject("incremental_sync");
            {
                builder.startObject("properties");
                builder.field("enabled", incrementalSyncEnabled);
                builder.endObject();
            }
            builder.endObject();
            builder.startObject("sync_rules");
            {
                builder.startObject("properties");
                {
                    builder.startObject("advanced");
                    {
                        builder.startObject("properties");
                        builder.field("enabled", syncRulesAdvancedEnabled);
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject("basic");
                    {
                        builder.startObject("properties");
                        builder.field("enabled", syncRulesBasicEnabled);
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(filteringAdvancedConfigEnabled);
        out.writeBoolean(filteringRulesEnabled);
        out.writeBoolean(incrementalSyncEnabled);
        out.writeBoolean(syncRulesAdvancedEnabled);
        out.writeBoolean(syncRulesBasicEnabled);
    }

    public static class Builder {

        private boolean filteringAdvancedConfig;
        private boolean filteringRules;
        private boolean incrementalSyncEnabled;
        private boolean syncRulesAdvancedEnabled;
        private boolean syncRulesBasicEnabled;

        public Builder setFilteringAdvancedConfig(boolean filteringAdvancedConfig) {
            this.filteringAdvancedConfig = filteringAdvancedConfig;
            return this;
        }

        public Builder setFilteringRules(boolean filteringRules) {
            this.filteringRules = filteringRules;
            return this;
        }

        public Builder setIncrementalSyncEnabled(boolean incrementalSyncEnabled) {
            this.incrementalSyncEnabled = incrementalSyncEnabled;
            return this;
        }

        public Builder setSyncRulesAdvancedEnabled(boolean syncRulesAdvancedEnabled) {
            this.syncRulesAdvancedEnabled = syncRulesAdvancedEnabled;
            return this;
        }

        public Builder setSyncRulesBasicEnabled(boolean syncRulesBasicEnabled) {
            this.syncRulesBasicEnabled = syncRulesBasicEnabled;
            return this;
        }

        public ConnectorFeatures createConnectorFeatures() {
            return new ConnectorFeatures(
                filteringAdvancedConfig,
                filteringRules,
                incrementalSyncEnabled,
                syncRulesAdvancedEnabled,
                syncRulesBasicEnabled
            );
        }
    }
}
