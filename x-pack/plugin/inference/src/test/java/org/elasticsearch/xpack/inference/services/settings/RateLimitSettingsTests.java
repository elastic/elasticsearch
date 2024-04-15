/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class RateLimitSettingsTests extends AbstractWireSerializingTestCase<RateLimitSettings> {

    public static RateLimitSettings createRandom() {
        return new RateLimitSettings(TimeValue.parseTimeValue(randomTimeValue(), "test"));
    }

    @Override
    protected Writeable.Reader<RateLimitSettings> instanceReader() {
        return RateLimitSettings::new;
    }

    @Override
    protected RateLimitSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected RateLimitSettings mutateInstance(RateLimitSettings instance) throws IOException {
        return createRandom();
    }
}
