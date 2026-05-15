/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.logging;

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

    private final EsqlLoggingConfig esqlConfig;

    LoggingFeatureSetUsage(EsqlLoggingConfig esqlConfig) {
        super(XPackField.LOGGING, true, true);
        this.esqlConfig = esqlConfig;
    }

    EsqlLoggingConfig esqlConfig() {
        return esqlConfig;
    }

    public LoggingFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        esqlConfig = new EsqlLoggingConfig(input);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        esqlConfig.writeTo(out);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
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
        return Objects.equals(esqlConfig, that.esqlConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(esqlConfig);
    }
}
