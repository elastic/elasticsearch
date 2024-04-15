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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalTimeValue;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;

public class RateLimitSettings implements Writeable, ToXContentFragment {

    public static final String FIELD_NAME = "rate_limit";
    public static final String REQUESTS_PER_TIME_UNIT_NAME = "requests_per_time_unit";

    private final TimeValue requestsPerTimeUnit;

    public static RateLimitSettings of(Map<String, Object> map, RateLimitSettings defaultValue, ValidationException validationException) {
        Map<String, Object> settings = removeFromMapOrDefaultEmpty(map, FIELD_NAME);
        var timeValue = extractOptionalTimeValue(settings, REQUESTS_PER_TIME_UNIT_NAME, FIELD_NAME, validationException);

        return timeValue == null ? defaultValue : new RateLimitSettings(timeValue);
    }

    public RateLimitSettings(TimeValue requestsPerTimeUnit) {
        this.requestsPerTimeUnit = Objects.requireNonNull(requestsPerTimeUnit);
    }

    public RateLimitSettings(StreamInput in) throws IOException {
        requestsPerTimeUnit = in.readTimeValue();
    }

    public long RequestsPerTimeUnit() {
        return requestsPerTimeUnit.duration();
    }

    public TimeUnit timeUnit() {
        return requestsPerTimeUnit.timeUnit();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELD_NAME);
        builder.field(REQUESTS_PER_TIME_UNIT_NAME, requestsPerTimeUnit);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(requestsPerTimeUnit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RateLimitSettings that = (RateLimitSettings) o;
        return Objects.equals(requestsPerTimeUnit, that.requestsPerTimeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestsPerTimeUnit);
    }
}
