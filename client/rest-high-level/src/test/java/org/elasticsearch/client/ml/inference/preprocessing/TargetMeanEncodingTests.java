/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;


public class TargetMeanEncodingTests extends AbstractXContentTestCase<TargetMeanEncoding> {

    @Override
    protected TargetMeanEncoding doParseInstance(XContentParser parser) throws IOException {
        return TargetMeanEncoding.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TargetMeanEncoding createTestInstance() {
        return createRandom();
    }

    public static TargetMeanEncoding createRandom() {
        return createRandom(randomAlphaOfLength(10));
    }

    public static TargetMeanEncoding createRandom(String inputField) {
        int valuesSize = randomIntBetween(1, 10);
        Map<String, Double> valueMap = new HashMap<>();
        for (int i = 0; i < valuesSize; i++) {
            valueMap.put(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, false));
        }
        return new TargetMeanEncoding(inputField,
            randomAlphaOfLength(10),
            valueMap,
            randomDoubleBetween(0.0, 1.0, false),
            true);
    }

}
