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

    static final String FIELD_NAME = "rate_limit";
    static final String TOKENS_PER_TIME_UNIT_NAME = "tokens_per_time_unit";

    private final TimeValue tokensPerTimeUnit;

    public static RateLimitSettings of(Map<String, Object> map, RateLimitSettings defaultValue, ValidationException validationException) {
        Map<String, Object> settings = removeFromMapOrDefaultEmpty(map, FIELD_NAME);
        var timeValue = extractOptionalTimeValue(settings, TOKENS_PER_TIME_UNIT_NAME, FIELD_NAME, validationException);

        return timeValue == null ? defaultValue : new RateLimitSettings(timeValue);
    }

    public RateLimitSettings(TimeValue tokensPerTimeUnit) {
        this.tokensPerTimeUnit = Objects.requireNonNull(tokensPerTimeUnit);
    }

    public RateLimitSettings(StreamInput in) throws IOException {
        tokensPerTimeUnit = in.readTimeValue();
    }

    public long tokensPerTimeUnit() {
        return tokensPerTimeUnit.duration();
    }

    public TimeUnit timeUnit() {
        return tokensPerTimeUnit.timeUnit();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELD_NAME);
        builder.field(TOKENS_PER_TIME_UNIT_NAME, tokensPerTimeUnit);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(tokensPerTimeUnit);
    }
}
