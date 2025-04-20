/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveLong;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class RateLimitSettings implements Writeable, ToXContentFragment {
    public static final String FIELD_NAME = "rate_limit";
    public static final String REQUESTS_PER_MINUTE_FIELD = "requests_per_minute";

    private final long requestsPerTimeUnit;
    private final TimeUnit timeUnit;

    public static RateLimitSettings of(
        Map<String, Object> map,
        RateLimitSettings defaultValue,
        ValidationException validationException,
        String serviceName,
        ConfigurationParseContext context
    ) {
        Map<String, Object> settings = removeFromMapOrDefaultEmpty(map, FIELD_NAME);
        var requestsPerMinute = extractOptionalPositiveLong(settings, REQUESTS_PER_MINUTE_FIELD, FIELD_NAME, validationException);

        if (ConfigurationParseContext.isRequestContext(context)) {
            throwIfNotEmptyMap(settings, serviceName);
        }

        return requestsPerMinute == null ? defaultValue : new RateLimitSettings(requestsPerMinute);
    }

    public static Map<String, SettingsConfiguration> toSettingsConfigurationWithDescription(
        String description,
        EnumSet<TaskType> supportedTaskTypes
    ) {
        var configurationMap = new HashMap<String, SettingsConfiguration>();
        configurationMap.put(
            FIELD_NAME + "." + REQUESTS_PER_MINUTE_FIELD,
            new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(description)
                .setLabel("Rate Limit")
                .setRequired(false)
                .setSensitive(false)
                .setUpdatable(false)
                .setType(SettingsConfigurationFieldType.INTEGER)
                .build()
        );
        return configurationMap;
    }

    public static Map<String, SettingsConfiguration> toSettingsConfiguration(EnumSet<TaskType> supportedTaskTypes) {
        return RateLimitSettings.toSettingsConfigurationWithDescription("Minimize the number of rate limit errors.", supportedTaskTypes);
    }

    /**
     * Defines the settings in requests per minute
     * @param requestsPerMinute _
     */
    public RateLimitSettings(long requestsPerMinute) {
        this(requestsPerMinute, TimeUnit.MINUTES);
    }

    /**
     * Defines the settings in requests per the time unit provided
     * @param requestsPerTimeUnit number of requests
     * @param timeUnit _
     *
     * Note: The time unit is not serialized
     */
    public RateLimitSettings(long requestsPerTimeUnit, TimeUnit timeUnit) {
        if (requestsPerTimeUnit <= 0) {
            throw new IllegalArgumentException("requests per minute must be positive");
        }
        this.requestsPerTimeUnit = requestsPerTimeUnit;
        this.timeUnit = Objects.requireNonNull(timeUnit);
    }

    public RateLimitSettings(StreamInput in) throws IOException {
        requestsPerTimeUnit = in.readVLong();
        timeUnit = TimeUnit.MINUTES;
    }

    public long requestsPerTimeUnit() {
        return requestsPerTimeUnit;
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELD_NAME);
        builder.field(REQUESTS_PER_MINUTE_FIELD, requestsPerTimeUnit);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(requestsPerTimeUnit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RateLimitSettings that = (RateLimitSettings) o;
        return Objects.equals(requestsPerTimeUnit, that.requestsPerTimeUnit) && Objects.equals(timeUnit, that.timeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestsPerTimeUnit, timeUnit);
    }
}
