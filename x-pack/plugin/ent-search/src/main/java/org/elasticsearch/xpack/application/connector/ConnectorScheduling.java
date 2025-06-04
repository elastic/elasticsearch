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
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.scheduler.Cron;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class ConnectorScheduling implements Writeable, ToXContentObject {

    private static final String EVERYDAY_AT_MIDNIGHT = "0 0 0 * * ?";
    private static final ParseField ACCESS_CONTROL_FIELD = new ParseField("access_control");
    private static final ParseField FULL_FIELD = new ParseField("full");
    private static final ParseField INCREMENTAL_FIELD = new ParseField("incremental");

    private final ScheduleConfig accessControl;
    private final ScheduleConfig full;
    private final ScheduleConfig incremental;

    /**
     * @param accessControl connector access control sync schedule represented as {@link ScheduleConfig}
     * @param full          connector full sync schedule represented as {@link ScheduleConfig}
     * @param incremental   connector incremental sync schedule represented as {@link ScheduleConfig}
     */
    private ConnectorScheduling(ScheduleConfig accessControl, ScheduleConfig full, ScheduleConfig incremental) {
        this.accessControl = accessControl;
        this.full = full;
        this.incremental = incremental;
    }

    public ConnectorScheduling(StreamInput in) throws IOException {
        this.accessControl = new ScheduleConfig(in);
        this.full = new ScheduleConfig(in);
        this.incremental = new ScheduleConfig(in);
    }

    public ScheduleConfig getAccessControl() {
        return accessControl;
    }

    public ScheduleConfig getFull() {
        return full;
    }

    public ScheduleConfig getIncremental() {
        return incremental;
    }

    private static final ConstructingObjectParser<ConnectorScheduling, Void> PARSER = new ConstructingObjectParser<>(
        "connector_scheduling",
        true,
        args -> new Builder().setAccessControl((ScheduleConfig) args[0])
            .setFull((ScheduleConfig) args[1])
            .setIncremental((ScheduleConfig) args[2])
            .build()
    );

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ScheduleConfig.fromXContent(p),
            ACCESS_CONTROL_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(optionalConstructorArg(), (p, c) -> ScheduleConfig.fromXContent(p), FULL_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ScheduleConfig.fromXContent(p),
            INCREMENTAL_FIELD,
            ObjectParser.ValueType.OBJECT
        );
    }

    public static ConnectorScheduling fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ConnectorScheduling.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    public static ConnectorScheduling fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (accessControl != null) {
                builder.field(ACCESS_CONTROL_FIELD.getPreferredName(), accessControl);
            }
            if (full != null) {
                builder.field(FULL_FIELD.getPreferredName(), full);
            }
            if (incremental != null) {
                builder.field(INCREMENTAL_FIELD.getPreferredName(), incremental);
            }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorScheduling that = (ConnectorScheduling) o;
        return Objects.equals(accessControl, that.accessControl)
            && Objects.equals(full, that.full)
            && Objects.equals(incremental, that.incremental);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessControl, full, incremental);
    }

    public static class Builder {

        private ScheduleConfig accessControl;
        private ScheduleConfig full;
        private ScheduleConfig incremental;

        public Builder setAccessControl(ScheduleConfig accessControl) {
            this.accessControl = accessControl;
            return this;
        }

        public Builder setFull(ScheduleConfig full) {
            this.full = full;
            return this;
        }

        public Builder setIncremental(ScheduleConfig incremental) {
            this.incremental = incremental;
            return this;
        }

        public ConnectorScheduling build() {
            return new ConnectorScheduling(accessControl, full, incremental);
        }
    }

    public static class ScheduleConfig implements Writeable, ToXContentObject {
        private final boolean enabled;
        private final Cron interval;

        private static final ParseField ENABLED_FIELD = new ParseField("enabled");
        private static final ParseField INTERVAL_FIELD = new ParseField("interval");

        /**
         * @param enabled  flag to disable/enable scheduling
         * @param interval CRON expression representing the sync schedule
         */
        private ScheduleConfig(boolean enabled, Cron interval) {
            this.enabled = enabled;
            this.interval = Objects.requireNonNull(interval, INTERVAL_FIELD.getPreferredName());
        }

        public ScheduleConfig(StreamInput in) throws IOException {
            this.enabled = in.readBoolean();
            this.interval = new Cron(in.readString());
        }

        private static final ConstructingObjectParser<ScheduleConfig, Void> PARSER = new ConstructingObjectParser<>(
            "schedule_config",
            true,
            args -> new Builder().setEnabled((boolean) args[0]).setInterval(new Cron((String) args[1])).build()
        );

        static {
            PARSER.declareBoolean(constructorArg(), ENABLED_FIELD);
            PARSER.declareString(constructorArg(), INTERVAL_FIELD);
        }

        public static ScheduleConfig fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
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
            out.writeString(interval.expression());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScheduleConfig that = (ScheduleConfig) o;
            return enabled == that.enabled && Objects.equals(interval, that.interval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled, interval);
        }

        public static class Builder {

            private boolean enabled;
            private Cron interval;

            public Builder setEnabled(boolean enabled) {
                this.enabled = enabled;
                return this;
            }

            public Builder setInterval(Cron interval) {
                this.interval = interval;
                return this;
            }

            public ScheduleConfig build() {
                return new ScheduleConfig(enabled, interval);
            }
        }
    }

    /**
     * Default scheduling is set to everyday at midnight (00:00:00).
     *
     * @return default scheduling for full, incremental and access control syncs.
     */
    public static ConnectorScheduling getDefaultConnectorScheduling() {
        return new ConnectorScheduling.Builder().setAccessControl(
            new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(false).setInterval(new Cron(EVERYDAY_AT_MIDNIGHT)).build()
        )
            .setFull(new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(false).setInterval(new Cron(EVERYDAY_AT_MIDNIGHT)).build())
            .setIncremental(
                new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(false).setInterval(new Cron(EVERYDAY_AT_MIDNIGHT)).build()
            )
            .build();
    }
}
