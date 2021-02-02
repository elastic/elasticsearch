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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RollupIndexMetadataTests extends AbstractSerializingTestCase<RollupIndexMetadata> {

    @Override
    protected RollupIndexMetadata doParseInstance(XContentParser parser) throws IOException {
        return RollupIndexMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupIndexMetadata> instanceReader() {
        return RollupIndexMetadata::new;
    }

    @Override
    protected RollupIndexMetadata createTestInstance() {
        return randomInstance();
    }

    static RollupIndexMetadata randomInstance() {
        DateHistogramInterval interval = randomFrom(DateHistogramInterval.MINUTE, DateHistogramInterval.HOUR);
        WriteableZoneId dateTimezone = WriteableZoneId.of(randomFrom(ZoneOffset.getAvailableZoneIds()));
        Map<String, List<String>> metrics = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            metrics.put(randomAlphaOfLength(5 + i),
                randomList(5, () -> randomFrom("min", "max", "sum", "value_count", "avg")));
        }

        return new RollupIndexMetadata(interval, dateTimezone, metrics);
    }
}
