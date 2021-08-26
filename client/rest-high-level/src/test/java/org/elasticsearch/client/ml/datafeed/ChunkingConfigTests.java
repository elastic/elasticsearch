/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ChunkingConfigTests extends AbstractXContentTestCase<ChunkingConfig> {

    @Override
    protected ChunkingConfig createTestInstance() {
        return createRandomizedChunk();
    }

    @Override
    protected ChunkingConfig doParseInstance(XContentParser parser) {
        return ChunkingConfig.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static ChunkingConfig createRandomizedChunk() {
        ChunkingConfig.Mode mode = randomFrom(ChunkingConfig.Mode.values());
        TimeValue timeSpan = null;
        if (mode == ChunkingConfig.Mode.MANUAL) {
            // time span is required to be at least 1 millis, so we use a custom method to generate a time value here
            timeSpan = randomPositiveSecondsMinutesHours();
        }
        return new ChunkingConfig(mode, timeSpan);
    }

    private static TimeValue randomPositiveSecondsMinutesHours() {
        return new TimeValue(randomIntBetween(1, 1000), randomFrom(Arrays.asList(TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS)));
    }

}
