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
