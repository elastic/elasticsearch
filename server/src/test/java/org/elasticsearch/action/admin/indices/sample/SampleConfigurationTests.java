/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.sample;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SampleConfigurationTests extends AbstractWireSerializingTestCase<SampleConfiguration> {

    @Override
    protected Writeable.Reader<SampleConfiguration> instanceReader() {
        return SampleConfiguration::new;
    }

    @Override
    protected SampleConfiguration createTestInstance() {
        return new SampleConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SampleConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofGb(randomLongBetween(1, SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SampleConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected SampleConfiguration mutateInstance(SampleConfiguration instance) {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> new SampleConfiguration(
                randomValueOtherThan(instance.rate(), () -> randomDoubleBetween(0.0, 1.0, true)),
                instance.maxSamples(),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 1 -> new SampleConfiguration(
                instance.rate(),
                randomValueOtherThan(instance.maxSamples(), () -> randomIntBetween(1, SampleConfiguration.MAX_SAMPLES_LIMIT)),
                instance.maxSize(),
                instance.timeToLive(),
                instance.condition()
            );
            case 2 -> new SampleConfiguration(
                instance.rate(),
                instance.maxSamples(),
                randomValueOtherThan(
                    instance.maxSize(),
                    () -> ByteSizeValue.ofGb(randomLongBetween(1, SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES))
                ),
                instance.timeToLive(),
                instance.condition()
            );
            case 3 -> new SampleConfiguration(
                instance.rate(),
                instance.maxSamples(),
                instance.maxSize(),
                randomValueOtherThan(
                    instance.timeToLive(),
                    () -> TimeValue.timeValueDays(randomLongBetween(1, SampleConfiguration.MAX_TIME_TO_LIVE_DAYS))
                ),
                instance.condition()
            );
            case 4 -> new SampleConfiguration(
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
        SampleConfiguration config = new SampleConfiguration(0.5, null, null, null, null);
        assertThat(config.rate(), equalTo(0.5));
        assertThat(config.maxSamples(), equalTo(SampleConfiguration.DEFAULT_MAX_SAMPLES));
        assertThat(config.maxSize(), equalTo(ByteSizeValue.ofGb(SampleConfiguration.DEFAULT_MAX_SIZE_GIGABYTES)));
        assertThat(config.timeToLive(), equalTo(TimeValue.timeValueDays(SampleConfiguration.DEFAULT_TIME_TO_LIVE_DAYS)));
        assertThat(config.condition(), nullValue());
    }

    public void testValidation() {
        // Test invalid rate
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SampleConfiguration(-0.1, null, null, null, null)
        );
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_RATE_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(1.1, null, null, null, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_RATE_MESSAGE));

        // Test invalid maxSamples
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, 0, null, null, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, -1, null, null, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SAMPLES_MIN_MESSAGE));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SampleConfiguration(0.5, SampleConfiguration.MAX_SAMPLES_LIMIT + 1, null, null, null)
        );
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SAMPLES_MAX_MESSAGE));

        // Test invalid maxSize
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, null, ByteSizeValue.ZERO, null, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, null, ByteSizeValue.ofBytes(-1), null, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SIZE_MIN_MESSAGE));
        ByteSizeValue maxSizeLimit = ByteSizeValue.ofGb(SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SampleConfiguration(0.5, null, ByteSizeValue.ofGb(SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES + 1), null, null)
        );
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_MAX_SIZE_MAX_MESSAGE));

        // Test invalid timeToLive
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, null, null, TimeValue.ZERO, null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE));
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, null, null, TimeValue.timeValueDays(-1), null));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_TIME_TO_LIVE_MIN_MESSAGE));
        TimeValue maxTimeLimit = TimeValue.timeValueDays(SampleConfiguration.MAX_TIME_TO_LIVE_DAYS);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new SampleConfiguration(0.5, null, null, TimeValue.timeValueDays(SampleConfiguration.MAX_TIME_TO_LIVE_DAYS + 1), null)
        );
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_TIME_TO_LIVE_MAX_MESSAGE));

        // Test invalid condition
        e = expectThrows(IllegalArgumentException.class, () -> new SampleConfiguration(0.5, null, null, null, ""));
        assertThat(e.getMessage(), equalTo(SampleConfiguration.INVALID_CONDITION_MESSAGE));
    }

    public void testValidInputs() {
        // Test boundary conditions
        new SampleConfiguration(0.001, 1, ByteSizeValue.ofBytes(1), TimeValue.timeValueMillis(1), "a"); // minimum values
        new SampleConfiguration(
            1.0,
            SampleConfiguration.MAX_SAMPLES_LIMIT,
            ByteSizeValue.ofGb(SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES),
            TimeValue.timeValueDays(SampleConfiguration.MAX_TIME_TO_LIVE_DAYS),
            "condition"
        ); // maximum values

        // Test random valid values
        new SampleConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomIntBetween(1, SampleConfiguration.MAX_SAMPLES_LIMIT),
            ByteSizeValue.ofGb(randomLongBetween(1, SampleConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            TimeValue.timeValueDays(randomLongBetween(1, SampleConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomAlphaOfLength(10)
        );
    }
}
