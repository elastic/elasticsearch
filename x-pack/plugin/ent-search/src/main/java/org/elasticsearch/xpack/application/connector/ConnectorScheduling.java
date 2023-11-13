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

public class ConnectorScheduling implements Writeable, ToXContentObject {

    private ScheduleConfig accessControl;
    private ScheduleConfig full;
    private ScheduleConfig incremental;

    public static final ParseField ACCESS_CONTROL_FIELD = new ParseField("access_control");
    public static final ParseField FULL_FIELD = new ParseField("full");
    public static final ParseField INCREMENTAL_FIELD = new ParseField("incremental");

    // Constructors, getters, and setters for ConnectorScheduling
    public ConnectorScheduling(ScheduleConfig accessControl, ScheduleConfig full, ScheduleConfig incremental) {
        this.accessControl = accessControl;
        this.full = full;
        this.incremental = incremental;
    }

    public ConnectorScheduling(StreamInput in) throws IOException {
        this.accessControl = readScheduleConfigFromStream(in);
        this.full = readScheduleConfigFromStream(in);
        this.incremental = readScheduleConfigFromStream(in);
    }

    private ScheduleConfig readScheduleConfigFromStream(StreamInput in) throws IOException {
        boolean enabled = in.readBoolean();
        String interval = in.readString();
        return new ScheduleConfig(enabled, interval);
    }

    private static final ConstructingObjectParser<ConnectorScheduling, Void> PARSER = new ConstructingObjectParser<>(
        "connector_scheduling",
        true,
        args -> new ConnectorScheduling((ScheduleConfig) args[0], (ScheduleConfig) args[1], (ScheduleConfig) args[2])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ScheduleConfig.getParser(), ACCESS_CONTROL_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ScheduleConfig.getParser(), FULL_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), ScheduleConfig.getParser(), INCREMENTAL_FIELD);
    }

    public static ConnectorScheduling fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("connector_scheduling");
        {
            builder.startObject(ACCESS_CONTROL_FIELD.getPreferredName());
            accessControl.toXContent(builder, params);
            builder.endObject();

            builder.startObject(FULL_FIELD.getPreferredName());
            full.toXContent(builder, params);
            builder.endObject();

            builder.startObject(INCREMENTAL_FIELD.getPreferredName());
            incremental.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        accessControl.writeTo(out);
        full.writeTo(out);
        incremental.writeTo(out);
    }

    public static class ScheduleConfig implements Writeable, ToXContentObject {
        private boolean enabled;
        private String interval;

        public static final ParseField ENABLED_FIELD = new ParseField("enabled");
        public static final ParseField INTERVAL_FIELD = new ParseField("interval");

        // Constructor, getters, and setters
        public ScheduleConfig(boolean enabled, String interval) {
            this.enabled = enabled;
            this.interval = interval;
        }


        private static final ConstructingObjectParser<ScheduleConfig, Void> PARSER = new ConstructingObjectParser<>(
            "schedule_config",
            true,
            args -> new ScheduleConfig((boolean) args[0], (String) args[1])
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INTERVAL_FIELD);
        }

        public static ScheduleConfig fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public static ConstructingObjectParser<ScheduleConfig, Void> getParser() {
            return PARSER;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("schedule_config");
            {
                builder.field(ENABLED_FIELD.getPreferredName(), enabled);
                builder.field(INTERVAL_FIELD.getPreferredName(), interval);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
            out.writeString(interval);
        }
    }

}
