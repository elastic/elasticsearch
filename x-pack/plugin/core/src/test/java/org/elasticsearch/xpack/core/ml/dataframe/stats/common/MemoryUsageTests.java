/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MemoryUsageTests extends AbstractSerializingTestCase<MemoryUsage> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected MemoryUsage doParseInstance(XContentParser parser) throws IOException {
        return lenient ? MemoryUsage.LENIENT_PARSER.parse(parser, null) : MemoryUsage.STRICT_PARSER.parse(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    public static MemoryUsage createRandom() {
        return new MemoryUsage(
            randomAlphaOfLength(10),
            Instant.now(),
            randomNonNegativeLong(),
            randomBoolean() ? null : randomFrom(MemoryUsage.Status.values()),
            randomBoolean() ? null : randomNonNegativeLong()
        );
    }

    @Override
    protected Writeable.Reader<MemoryUsage> instanceReader() {
        return MemoryUsage::new;
    }

    @Override
    protected MemoryUsage createTestInstance() {
        return createRandom();
    }

    public void testZeroUsage() {
        MemoryUsage memoryUsage = new MemoryUsage("zero_usage_job");
        String asJson = Strings.toString(memoryUsage);
        assertThat(asJson, equalTo("{\"peak_usage_bytes\":0,\"status\":\"ok\"}"));
    }
}
