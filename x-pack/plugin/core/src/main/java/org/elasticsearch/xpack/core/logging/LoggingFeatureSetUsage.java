/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class LoggingFeatureSetUsage extends XPackFeatureUsage {

    private static final TransportVersion LOGGING_XPACK_USAGE = TransportVersion.fromName("logging_xpack_usage");

    record LoggingConfig(boolean enabled, boolean userInfo) implements Writeable, ToXContentFragment {

        LoggingConfig(StreamInput in) throws IOException {
            this(in.readBoolean(), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
            out.writeBoolean(userInfo);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("enabled", enabled);
            builder.field("user", userInfo);
            return builder;
        }
    }

    record EsqlLoggingConfig(LoggingConfig base, Map<String, String> thresholds) implements Writeable, ToXContentObject {

        EsqlLoggingConfig(StreamInput input) throws IOException {
            this(new LoggingConfig(input), input.readMap(StreamInput::readString, StreamInput::readString));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            base.writeTo(out);
            out.writeMap(thresholds, StreamOutput::writeString, StreamOutput::writeString);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            base.toXContent(builder, params);
            if (thresholds.isEmpty() == false) {
                builder.field("thresholds", thresholds);
            }
            builder.endObject();
            return builder;
        }
    }

    record QueryLoggingConfig(LoggingConfig base, boolean system, String threshold) implements Writeable, ToXContentObject {

        QueryLoggingConfig(StreamInput input) throws IOException {
            this(new LoggingConfig(input), input.readBoolean(), input.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            base.writeTo(out);
            out.writeBoolean(system);
            out.writeOptionalString(threshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            base.toXContent(builder, params);
            builder.field("system", system);
            if (Strings.isNotEmpty(threshold)) {
                builder.field("threshold", threshold);
            }
            builder.endObject();
            return builder;
        }
    }

    private final EsqlLoggingConfig esqlConfig;
    private final QueryLoggingConfig queryConfig;

    LoggingFeatureSetUsage(QueryLoggingConfig queryConfig, EsqlLoggingConfig esqlConfig) {
        super(XPackField.LOGGING, true, true);
        this.esqlConfig = esqlConfig;
        this.queryConfig = queryConfig;
    }

    QueryLoggingConfig queryConfig() {
        return queryConfig;
    }

    EsqlLoggingConfig esqlConfig() {
        return esqlConfig;
    }

    public LoggingFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        esqlConfig = new EsqlLoggingConfig(input);
        queryConfig = new QueryLoggingConfig(input);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        esqlConfig.writeTo(out);
        queryConfig.writeTo(out);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("querylog", queryConfig);
        builder.field("esql", esqlConfig);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return LOGGING_XPACK_USAGE;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoggingFeatureSetUsage that = (LoggingFeatureSetUsage) o;
        return Objects.equals(esqlConfig, that.esqlConfig) && Objects.equals(queryConfig, that.queryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(esqlConfig, queryConfig);
    }
}
