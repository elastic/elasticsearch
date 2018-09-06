/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.filestructurefinder;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldStatsTests extends AbstractXContentTestCase<FieldStats> {

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
        boolean isMetric = randomBoolean();
        if (isMetric) {
            minValue = randomDouble();
            maxValue = randomDouble();
            meanValue = randomDouble();
            medianValue = randomDouble();
        }

        List<Map<String, Object>> topHits = new ArrayList<>();
        for (int i = 0; i < Math.min(10, cardinality); ++i) {
            Map<String, Object> topHit = new LinkedHashMap<>();
            if (isMetric) {
                topHit.put("value", randomDouble());
            } else {
                topHit.put("value", randomAlphaOfLength(20));
            }
            topHit.put("count", randomIntBetween(1, cardinality));
            topHits.add(topHit);
        }

        return new FieldStats(count, cardinality, minValue, maxValue, meanValue, medianValue, topHits);
    }

    protected FieldStats doParseInstance(XContentParser parser) {
        return FieldStats.PARSER.apply(parser, null);
    }

    protected boolean supportsUnknownFields() {
        return false;
    }
}
