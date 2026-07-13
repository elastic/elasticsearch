/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.util.HashMap;
import java.util.Map;

public class RowResultsTests extends AbstractXContentTestCase<RowResults> {

    @Override
    protected RowResults createTestInstance() {
        return createRandom();
    }

    public static RowResults createRandom() {
        int checksum = randomInt();
        Map<String, Object> results = new HashMap<>();
        int resultsSize = randomIntBetween(1, 10);
        for (int i = 0; i < resultsSize; i++) {
            String resultField = randomAlphaOfLength(20);
            Object resultValue = randomBoolean() ? randomAlphaOfLength(20) : randomDouble();
            results.put(resultField, resultValue);
        }
        return new RowResults(checksum, results);
    }

    @Override
    protected RowResults doParseInstance(XContentParser parser) {
        return RowResults.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
