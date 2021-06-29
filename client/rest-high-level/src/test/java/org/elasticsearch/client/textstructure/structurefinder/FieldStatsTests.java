/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.textstructure.structurefinder;

import org.elasticsearch.client.textstructure.structurefinder.FieldStats;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class FieldStatsTests extends AbstractXContentTestCase<FieldStats> {

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
    protected FieldStats doParseInstance(XContentParser parser) {
        return FieldStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.contains(FieldStats.TOP_HITS.getPreferredName());
    }
}
