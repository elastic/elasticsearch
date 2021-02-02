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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RollupGroupTests extends AbstractSerializingTestCase<RollupGroup> {

    @Override
    protected RollupGroup doParseInstance(XContentParser parser) throws IOException {
        return RollupGroup.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupGroup> instanceReader() {
        return RollupGroup::new;
    }

    @Override
    protected RollupGroup createTestInstance() {
        return randomInstance();
    }

    static RollupGroup randomInstance() {
        List<String> group = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            group.add(randomAlphaOfLength(5 + i));
        }
        Map<String, DateHistogramInterval> dateInterval = new HashMap<>();
        Map<String, WriteableZoneId> dateTimezone = new HashMap<>();
        for (String index : group) {
            DateHistogramInterval interval = randomFrom(DateHistogramInterval.MINUTE, DateHistogramInterval.HOUR);
            dateInterval.put(index, interval);
            dateTimezone.put(index, WriteableZoneId.of(randomFrom(ZoneOffset.getAvailableZoneIds())));
        }
        return new RollupGroup(group, dateInterval, dateTimezone);
    }
}
