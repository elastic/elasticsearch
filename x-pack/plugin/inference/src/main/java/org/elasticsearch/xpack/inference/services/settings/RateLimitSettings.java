/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
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

public class RateLimitSettings implements Writeable, ToXContentFragment {
    public static final String FIELD_NAME = "rate_limit";
    public static final String REQUESTS_PER_MINUTE_FIELD = "requests_per_minute";
    public static final RateLimitSettings DISABLED_INSTANCE = new RateLimitSettings(1, TimeUnit.MINUTES, false);

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

    /**
     * Creates a new instance of {@link RateLimitSettings} from the provided map.
     * The map is expected to contain a nested map under the key defined by {@link #FIELD_NAME}.
     * If the nested map contains the key defined by {@link #REQUESTS_PER_MINUTE_FIELD},
     * its value is extracted and used to create a new instance of {@link RateLimitSettings}.
     * If the nested map does not contain the key or if the value is null, the provided default value is returned.
     * @param serviceSettingsMap the map containing the service settings,
     *                           expected to contain a nested map for rate limit settings under the key defined by {@link #FIELD_NAME}
     * @param defaultValue the default value to return if the map does not contain valid rate limit settings
     * @param validationException the {@link ValidationException} to which validation errors will be added
     *                            if the map contains invalid rate limit settings
     * @param context the context of the configuration parsing, used to determine if validation should be applied
     * @return a new instance of {@link RateLimitSettings} based on the provided map,
     * or the default value if the map does not contain valid rate limit settings
     */
    public static RateLimitSettings of(
        Map<String, Object> serviceSettingsMap,
        RateLimitSettings defaultValue,
        ValidationException validationException,
        ConfigurationParseContext context
    ) {
        var rateLimitSettings = removeFromMapOrDefaultEmpty(serviceSettingsMap, FIELD_NAME);
        var requestsPerMinute = extractOptionalPositiveLong(rateLimitSettings, REQUESTS_PER_MINUTE_FIELD, FIELD_NAME, validationException);

        if (ConfigurationParseContext.isRequestContext(context) && rateLimitSettings.isEmpty() == false) {
            validationException.addValidationError(
                Strings.format("Rate limit settings contain unknown entries [%s]", rateLimitSettings.toString())
            );
            return defaultValue;
        }

        return requestsPerMinute == null ? defaultValue : new RateLimitSettings(requestsPerMinute);
    }

    /**
     * Validates that rate limit settings are not included in the request context.
     * If rate limit settings are found, a validation error is added to the provided {@link ValidationException}.
     * Currently used only for {@code elastic} provider that doesn't support rate limit settings.
     * @param map the map containing the settings to validate
     * @param scope the scope of the settings, used for error messaging
     * @param service the name of the service, used for error messaging
     * @param taskType the task type, used for error messaging
     * @param context the context of the configuration parsing, used to determine if validation should be applied
     * @param validationException the {@link ValidationException} to which validation errors will be added
     *                            if rate limit settings are found in the request context
     */
    public static void rejectRateLimitFieldForRequestContext(
        Map<String, Object> map,
        String scope,
        String service,
        TaskType taskType,
        ConfigurationParseContext context,
        ValidationException validationException
    ) {
        if (ConfigurationParseContext.isRequestContext(context) && map.containsKey(FIELD_NAME)) {
            validationException.addValidationError(
                Strings.format(
                    "[%s] rate limit settings are not permitted for service [%s] and task type [%s]",
                    scope,
                    service,
                    taskType.toString()
                )
            );
        }
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

    private final long requestsPerTimeUnit;
    private final TimeUnit timeUnit;
    private final boolean enabled;

    /**
     * Defines the settings in requests per minute
     * @param requestsPerMinute _
     */
    public RateLimitSettings(long requestsPerMinute) {
        this(requestsPerMinute, TimeUnit.MINUTES);
    }

    /**
     * This should only be used for testing.
     *
     * Defines the settings in requests per the time unit provided
     * @param requestsPerTimeUnit number of requests
     * @param timeUnit _
     *
     * Note: The time unit is not serialized
     */
    public RateLimitSettings(long requestsPerTimeUnit, TimeUnit timeUnit) {
        this(requestsPerTimeUnit, timeUnit, true);
    }

    // This should only be used for testing.
    public RateLimitSettings(long requestsPerTimeUnit, TimeUnit timeUnit, boolean enabled) {
        if (requestsPerTimeUnit <= 0) {
            throw new IllegalArgumentException("requests per minute must be positive");
        }
        this.requestsPerTimeUnit = requestsPerTimeUnit;
        this.timeUnit = Objects.requireNonNull(timeUnit);
        this.enabled = enabled;
    }

    public RateLimitSettings(StreamInput in) throws IOException {
        requestsPerTimeUnit = in.readVLong();
        timeUnit = TimeUnit.MINUTES;
        if (in.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING)) {
            enabled = in.readBoolean();
        } else {
            enabled = true;
        }
    }

    public long requestsPerTimeUnit() {
        return requestsPerTimeUnit;
    }

    public TimeUnit timeUnit() {
        return timeUnit;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (enabled == false) {
            return builder;
        }

        builder.startObject(FIELD_NAME);
        builder.field(REQUESTS_PER_MINUTE_FIELD, requestsPerTimeUnit);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(requestsPerTimeUnit);
        if (out.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING)) {
            out.writeBoolean(enabled);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RateLimitSettings that = (RateLimitSettings) o;
        return Objects.equals(requestsPerTimeUnit, that.requestsPerTimeUnit)
            && Objects.equals(timeUnit, that.timeUnit)
            && enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestsPerTimeUnit, timeUnit, enabled);
    }
}
