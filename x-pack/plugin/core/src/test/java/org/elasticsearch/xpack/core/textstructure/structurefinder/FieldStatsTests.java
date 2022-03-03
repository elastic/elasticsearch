/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.structurefinder;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldStatsTests extends AbstractSerializingTestCase<FieldStats> {

    @Override
    protected FieldStats createTestInstance() {
        return createTestFieldStats();
    }

    static FieldStats createTestFieldStats() {

        long count = randomIntBetween(1, 100000);
        int cardinality = randomIntBetween(1, (int) count);

        Double minValue = null;
        Double maxValue = null;
        Double meanValue = null;
        Double medianValue = null;
        String earliestTimestamp = null;
        String latestTimestamp = null;
        boolean isMetric = randomBoolean();
        if (isMetric) {
            if (randomBoolean()) {
                minValue = randomDouble();
                maxValue = randomDouble();
            } else {
                minValue = (double) randomInt();
                maxValue = (double) randomInt();
            }
            meanValue = randomDouble();
            medianValue = randomDouble();
        } else {
            boolean isDate = randomBoolean();
            if (isDate) {
                earliestTimestamp = randomAlphaOfLength(20);
                latestTimestamp = randomAlphaOfLength(20);
            }
        }

        List<Map<String, Object>> topHits = new ArrayList<>();
        for (int i = 0; i < Math.min(10, cardinality); ++i) {
            Map<String, Object> topHit = new LinkedHashMap<>();
            if (isMetric) {
                topHit.put("value", randomBoolean() ? randomDouble() : (double) randomInt());
            } else {
                topHit.put("value", randomAlphaOfLength(20));
            }
            topHit.put("count", randomIntBetween(1, cardinality));
            topHits.add(topHit);
        }

        return new FieldStats(count, cardinality, minValue, maxValue, meanValue, medianValue, earliestTimestamp, latestTimestamp, topHits);
    }

    @Override
    protected Writeable.Reader<FieldStats> instanceReader() {
        return FieldStats::new;
    }

    @Override
    protected FieldStats doParseInstance(XContentParser parser) {
        return FieldStats.PARSER.apply(parser, null);
    }
}
