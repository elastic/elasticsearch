/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
