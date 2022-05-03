/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractSerializingTestCase<TimeRetentionPolicyConfig> {

    public static TimeRetentionPolicyConfig randomTimeRetentionPolicyConfig() {
        return new TimeRetentionPolicyConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomLongBetween(60000, 1_000_000_000L)));
    }

    @Override
    protected TimeRetentionPolicyConfig doParseInstance(XContentParser parser) throws IOException {
        return TimeRetentionPolicyConfig.fromXContent(parser, false);
    }

    @Override
    protected TimeRetentionPolicyConfig createTestInstance() {
        return randomTimeRetentionPolicyConfig();
    }

    @Override
    protected Reader<TimeRetentionPolicyConfig> instanceReader() {
        return TimeRetentionPolicyConfig::new;
    }

    public void testValidationMin() {
        TimeRetentionPolicyConfig timeRetentionPolicyConfig = new TimeRetentionPolicyConfig(
            randomAlphaOfLengthBetween(1, 10),
            TimeValue.timeValueSeconds(10)
        );

        ActionRequestValidationException e = timeRetentionPolicyConfig.validate(null);
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertEquals("retention_policy.time.max_age must be greater than 60s, found [10s]", e.validationErrors().get(0));
    }

    public void testValidationMax() {
        TimeRetentionPolicyConfig timeRetentionPolicyConfig = new TimeRetentionPolicyConfig(
            randomAlphaOfLengthBetween(1, 10),
            TimeValue.parseTimeValue("600000000000d", "time value")
        );

        ActionRequestValidationException e = timeRetentionPolicyConfig.validate(null);
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertEquals("retention_policy.time.max_age must not be greater than [106751.9d]", e.validationErrors().get(0));
    }
}
