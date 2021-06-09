/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class TimeRetentionPolicyConfigTests extends AbstractXContentTestCase<TimeRetentionPolicyConfig> {

    public static TimeRetentionPolicyConfig randomTimeRetentionPolicyConfig() {
        return new TimeRetentionPolicyConfig(randomAlphaOfLengthBetween(1, 10), new TimeValue(randomNonNegativeLong()));
    }

    @Override
    protected TimeRetentionPolicyConfig createTestInstance() {
        return randomTimeRetentionPolicyConfig();
    }

    @Override
    protected TimeRetentionPolicyConfig doParseInstance(XContentParser parser) throws IOException {
        return TimeRetentionPolicyConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
