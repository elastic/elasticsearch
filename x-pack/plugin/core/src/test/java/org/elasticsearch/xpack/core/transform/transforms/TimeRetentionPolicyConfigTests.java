/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractSerializingTestCase<TimeRetentionPolicyConfig> {

    public static TimeRetentionPolicyConfig randomTimeRetentionPolicyConfig() {
        return new TimeRetentionPolicyConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomNonNegativeLong()));
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
