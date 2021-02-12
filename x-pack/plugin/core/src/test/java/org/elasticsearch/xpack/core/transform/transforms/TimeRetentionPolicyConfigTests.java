/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractSerializingTestCase<TimeRetentionPolicyConfig> {

    public static TimeRetentionPolicyConfig randomTimeRetentionPolicyConfig() {
        return new TimeRetentionPolicyConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomLongBetween(60000, Long.MAX_VALUE)));
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
}
