/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SamplingConfigurationTests extends AbstractXContentSerializingTestCase<SamplingConfiguration> {

    @Override
    protected SamplingConfiguration doParseInstance(XContentParser parser) throws IOException {
        return SamplingConfiguration.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SamplingConfiguration> instanceReader() {
        return SamplingConfiguration::new;
    }

    @Override
    protected SamplingConfiguration createTestInstance() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofGb(randomLongBetween(1, SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected SamplingConfiguration mutateInstance(SamplingConfiguration instance) {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> new SamplingConfiguration(
                randomValueOtherThan(instance.rate(), () -> randomDoubleBetween(0.0, 1.0, true)),
                instance.maxSamples(),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 1 -> new SamplingConfiguration(
                instance.rate(),
                randomValueOtherThan(instance.maxSamples(), () -> randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT)),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 2 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                randomValueOtherThan(
                    instance.maxSize(),
                    () -> ByteSizeValue.ofGb(randomLongBetween(1, SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES))
                ),
                instance.timeToLive(),
                instance.condition()
            );
            case 3 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                instance.maxSize(),
                randomValueOtherThan(
                    instance.timeToLive(),
                    () -> TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS))
                ),
                instance.condition()
            );
            case 4 -> new SamplingConfiguration(
                instance.rate(),
                instance.maxSamples(),
                instance.maxSize(),
                instance.timeToLive(),
                randomValueOtherThan(instance.condition(), () -> randomAlphaOfLength(10))
            );
            default -> throw new AssertionError("Unexpected value");
        };
    }

    public void testDefaults() {
        SamplingConfiguration config = new SamplingConfiguration(0.5, null, null, null, null);
        assertThat(config.rate(), equalTo(0.5));
        assertThat(config.maxSamples(), equalTo(SamplingConfiguration.DEFAULT_MAX_SAMPLES));
        assertThat(config.maxSize(), equalTo(ByteSizeValue.ofGb(SamplingConfiguration.DEFAULT_MAX_SIZE_GIGABYTES)));
        assertThat(config.timeToLive(), equalTo(TimeValue.timeValueDays(SamplingConfiguration.DEFAULT_TIME_TO_LIVE_DAYS)));
        assertThat(config.condition(), nullValue());
    }

    public void testValidation() {
        // Test invalid rate
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SamplingConfiguration(-0.1, null, null, null, null)
        );
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_RATE_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(1.1, null, null, null, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_RATE_MESSAGE));

        // Test invalid maxSamples
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, 0, null, null, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, -1, null, null, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SamplingConfiguration(0.5, SamplingConfiguration.MAX_SAMPLES_LIMIT + 1, null, null, null)
        );
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SAMPLES_MAX_MESSAGE));

        // Test invalid maxSize
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, null, ByteSizeValue.ZERO, null, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, null, ByteSizeValue.ofBytes(-1), null, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE));
        ByteSizeValue maxSizeLimit = ByteSizeValue.ofGb(SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SamplingConfiguration(0.5, null, ByteSizeValue.ofGb(SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES + 1), null, null)
        );
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_MAX_SIZE_MAX_MESSAGE));

        // Test invalid timeToLive
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, null, null, TimeValue.ZERO, null));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SamplingConfiguration(0.5, null, null, TimeValue.timeValueDays(-1), null)
        );
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE));
        TimeValue maxTimeLimit = TimeValue.timeValueDays(SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SamplingConfiguration(0.5, null, null, TimeValue.timeValueDays(SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS + 1), null)
        );
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_TIME_TO_LIVE_MAX_MESSAGE));

        // Test invalid condition
        e = expectThrows(IllegalArgumentException.class, () -> new SamplingConfiguration(0.5, null, null, null, ""));
        assertThat(e.getMessage(), equalTo(SamplingConfiguration.INVALID_CONDITION_MESSAGE));
    }

    public void testValidInputs() {
        // Test boundary conditions
        new SamplingConfiguration(0.001, 1, ByteSizeValue.ofBytes(1), TimeValue.timeValueMillis(1), "a"); // minimum values
        new SamplingConfiguration(
            1.0,
            SamplingConfiguration.MAX_SAMPLES_LIMIT,
            ByteSizeValue.ofGb(SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES),
            TimeValue.timeValueDays(SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS),
            "condition"
        ); // maximum values

        // Test random valid values
        new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            ByteSizeValue.ofGb(randomLongBetween(1, SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomAlphaOfLength(10)
        );
    }
}
