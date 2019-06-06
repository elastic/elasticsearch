/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.common.unit.TimeValue;
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
