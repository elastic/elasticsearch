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

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsTypeOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeStringOrThrowIfNull;

public class DefaultServiceSettings implements ServiceSettings {
    private final InferenceHeadersAndBody headersAndBody;
    private final TaskType taskType;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;
    private final Integer tokenLimit;
    private final Integer rateLimitGroup;

    DefaultServiceSettings(
        InferenceHeadersAndBody headersAndBody,
        TaskType taskType,
        String modelId,
        RateLimitSettings rateLimitSettings,
        Integer tokenLimit,
        Integer rateLimitGroup
    ) {
        this.headersAndBody = headersAndBody;
        this.taskType = taskType;
        this.modelId = modelId;
        this.rateLimitSettings = rateLimitSettings;
        this.tokenLimit = tokenLimit;
        this.rateLimitGroup = rateLimitGroup;
    }

    DefaultServiceSettings(StreamInput in) throws IOException {
        this.headersAndBody = in.readOptionalWriteable(InferenceHeadersAndBody::new);
        this.taskType = TaskType.fromString(in.readString());
        this.modelId = in.readString();
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
        out.writeWriteable(headersAndBody);
        out.writeString(taskType.name());
        out.writeString(modelId);
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
        builder.startObject();
        builder.field("headersAndBody", headersAndBody);
        builder.field("taskType", taskType.name());
        builder.field("modelId", modelId);
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
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this; // do not filter anything
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public static DefaultServiceSettings fromStorage(Map<String, Object> storage) {
        var headersAndBody = InferenceHeadersAndBody.fromStorage(removeFromMapOrThrowIfNull(storage, "headersAndBody"));
        var taskType = TaskType.fromString(removeStringOrThrowIfNull(storage, "taskType"));
        var modelId = removeStringOrThrowIfNull(storage, "modelId");
        var tokenLimit = removeAsType(storage, "tokenLimit", Integer.class);
        var rateLimitSettings = rateLimitSettings(removeFromMapOrThrowIfNull(storage, "rateLimitSettings"));
        var rateLimitGroup = removeAsType(storage, "rateLimitGroup", Integer.class);
        return new DefaultServiceSettings(headersAndBody, taskType, modelId, rateLimitSettings, tokenLimit,  rateLimitGroup);
    }

    private static RateLimitSettings rateLimitSettings(Map<String, Object> storage) {
        var requestsPerTimeUnit = removeAsTypeOrThrowIfNull(storage, "requestsPerTimeUnit", Long.class);
        var timeUnit = TimeUnit.valueOf(removeStringOrThrowIfNull(storage, "timeUnit"));
        return new RateLimitSettings(requestsPerTimeUnit, timeUnit);
    }
}
