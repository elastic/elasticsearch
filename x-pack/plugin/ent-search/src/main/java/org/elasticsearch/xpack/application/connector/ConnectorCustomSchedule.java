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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class ConnectorCustomSchedule implements Writeable, ToXContentObject {

    private final Map<String, Object> configurationOverrides;
    private final boolean enabled;
    private final String interval;
    private final String lastSynced;
    private final String name;

    private ConnectorCustomSchedule(
        Map<String, Object> configurationOverrides,
        boolean enabled,
        String interval,
        String lastSynced,
        String name
    ) {
        this.configurationOverrides = configurationOverrides;
        this.enabled = enabled;
        this.interval = interval;
        this.lastSynced = lastSynced;
        this.name = name;
    }

    public ConnectorCustomSchedule(StreamInput in) throws IOException {
        this.configurationOverrides = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        this.enabled = in.readBoolean();
        this.interval = in.readString();
        this.lastSynced = in.readString();
        this.name = in.readString();
    }

    public static final ParseField CONFIG_OVERRIDES_FIELD = new ParseField("configuration_overrides");
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");
    public static final ParseField INTERVAL_FIELD = new ParseField("interval");
    public static final ParseField LAST_SYNCED_FIELD = new ParseField("last_synced");

    public static final ParseField NAME_FIELD = new ParseField("name");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ConnectorCustomSchedule, Void> PARSER = new ConstructingObjectParser<>(
        "connector_custom_schedule",
        true,
        args -> new Builder().setConfigurationOverrides((Map<String, Object>) args[0])
            .setEnabled((boolean) args[1])
            .setInterval((String) args[2])
            .setLastSynced((String) args[3])
            .setName((String) args[4])
            .createConnectorCustomSchedule()
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> parser.map(),
            new ParseField("configurationOverrides"),
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INTERVAL_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LAST_SYNCED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
    }

    public static ConnectorCustomSchedule fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (configurationOverrides != null) {
            builder.field(CONFIG_OVERRIDES_FIELD.getPreferredName(), configurationOverrides);
        }

        builder.field(ENABLED_FIELD.getPreferredName(), enabled);

        if (interval != null) {
            builder.field(INTERVAL_FIELD.getPreferredName(), interval);
        }

        if (lastSynced != null) {
            builder.field(LAST_SYNCED_FIELD.getPreferredName(), lastSynced);
        }

        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(configurationOverrides, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeBoolean(enabled);
        out.writeString(interval);
        out.writeString(lastSynced);
        out.writeString(name);
    }

    public static class Builder {

        private Map<String, Object> configurationOverrides;
        private boolean enabled;
        private String interval;
        private String lastSynced;
        private String name;

        public Builder setConfigurationOverrides(Map<String, Object> configurationOverrides) {
            this.configurationOverrides = configurationOverrides;
            return this;
        }

        public Builder setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder setInterval(String interval) {
            this.interval = interval;
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

        public ConnectorCustomSchedule createConnectorCustomSchedule() {
            return new ConnectorCustomSchedule(configurationOverrides, enabled, interval, lastSynced, name);
        }
    }
}
