/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;

public class MemoryUsageTests extends AbstractXContentTestCase<MemoryUsage> {

    @Override
    protected MemoryUsage createTestInstance() {
        return createRandom();
    }

    public static MemoryUsage createRandom() {
        return new MemoryUsage(
            randomBoolean() ? null : Instant.now(),
            randomNonNegativeLong(),
            randomFrom(MemoryUsage.Status.values()),
            randomBoolean() ? null : randomNonNegativeLong()
        );
    }

    @Override
    protected MemoryUsage doParseInstance(XContentParser parser) throws IOException {
        return MemoryUsage.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testToString_GivenNullTimestamp() {
        MemoryUsage memoryUsage = new MemoryUsage(null, 42L, MemoryUsage.Status.OK, null);
        assertThat(memoryUsage.toString(), equalTo(
            "MemoryUsage[timestamp=null, peak_usage_bytes=42, status=ok, memory_reestimate_bytes=null]"));
    }
}
