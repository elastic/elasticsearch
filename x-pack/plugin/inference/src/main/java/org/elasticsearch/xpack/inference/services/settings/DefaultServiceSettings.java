/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultServiceSettings extends SettingsMap implements ServiceSettings {
    private final TaskType taskType;
    private final RateLimitSettings rateLimitSettings;
    private final Integer tokenLimit;
    private final Integer rateLimitGroup;

    DefaultServiceSettings(
        Map<String, Object> headers,
        Map<String, Object> body,
        TaskType taskType,
        RateLimitSettings rateLimitSettings,
        Integer tokenLimit,
        Integer rateLimitGroup
    ) {
        super(headers, body);
        this.taskType = taskType;
        this.rateLimitSettings = rateLimitSettings;
        this.tokenLimit = tokenLimit;
        this.rateLimitGroup = rateLimitGroup;
    }

    DefaultServiceSettings(StreamInput in) throws IOException {
        super(in);
        this.taskType = TaskType.fromString(in.readString());
        this.rateLimitSettings = in.readOptionalWriteable(rateLimitSettingsReader());
        this.tokenLimit = in.readOptionalInt();
        this.rateLimitGroup = in.readOptionalInt();
    }

    private static Reader<RateLimitSettings> rateLimitSettingsReader() {
        return in -> {
            var requestsPerTimeUnit = in.readLong();
            var timeUnit = TimeUnit.valueOf(in.readString());
            return new RateLimitSettings(requestsPerTimeUnit, timeUnit);
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(taskType.name());
        out.writeOptionalWriteable(rateLimitSettingsWriteable());
        out.writeOptionalInt(tokenLimit);
        out.writeOptionalInt(rateLimitGroup);
    }

    private Writeable rateLimitSettingsWriteable() {
        return out -> {
            out.writeLong(rateLimitSettings.requestsPerTimeUnit());
            out.writeString(rateLimitSettings.timeUnit().name());
        };
    }

    @Override
    public String getWriteableName() {
        return "inference_default_service_settings";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field("taskType", taskType.name());
        builder.field("rateLimitSettings");
        builder.startObject();
        builder.field("requestsPerTimeUnit", rateLimitSettings.requestsPerTimeUnit());
        builder.field("timeUnit", rateLimitSettings.timeUnit().name());
        builder.endObject();
        if (tokenLimit != null) {
            builder.field("tokenLimit", tokenLimit);
        }
        if (rateLimitGroup != null) {
            builder.field("rateLimitGroup", rateLimitGroup);
        }
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this; // do not filter anything
    }
}
