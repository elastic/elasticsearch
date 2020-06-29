/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
