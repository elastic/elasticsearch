/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalyticsResultTests extends AbstractXContentTestCase<AnalyticsResult> {

    @Override
    protected AnalyticsResult createTestInstance() {
        int checksum = randomInt();
        Map<String, Object> results = new HashMap<>();
        int resultsSize = randomIntBetween(1, 10);
        for (int i = 0; i < resultsSize; i++) {
            String resultField = randomAlphaOfLength(20);
            Object resultValue = randomBoolean() ? randomAlphaOfLength(20) : randomDouble();
            results.put(resultField, resultValue);
        }
        return new AnalyticsResult(checksum, results);
    }

    @Override
    protected AnalyticsResult doParseInstance(XContentParser parser) throws IOException {
        return AnalyticsResult.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
