/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneHotEncodingSizeTests extends SizeEstimatorTestCase<OneHotEncodingSize, OneHotEncoding> {

    static OneHotEncodingSize createRandom() {
        int numFieldEntries = randomIntBetween(1, 10);
        return new OneHotEncodingSize(
            randomInt(100),
            Stream.generate(() -> randomIntBetween(5, 10))
                .limit(numFieldEntries)
                .collect(Collectors.toList()),
            Stream.generate(() -> randomIntBetween(5, 10))
                .limit(numFieldEntries)
                .collect(Collectors.toList()));
    }

    static OneHotEncodingSize translateToEstimate(OneHotEncoding encoding) {
        return new OneHotEncodingSize(encoding.getField().length(),
            encoding.getHotMap().values().stream().map(String::length).collect(Collectors.toList()),
            encoding.getHotMap().keySet().stream().map(String::length).collect(Collectors.toList()));
    }

    @Override
    protected OneHotEncodingSize createTestInstance() {
        return createRandom();
    }

    @Override
    protected OneHotEncodingSize doParseInstance(XContentParser parser) {
        return OneHotEncodingSize.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    OneHotEncoding generateTrueObject() {
        return OneHotEncodingTests.createRandom();
    }

    @Override
    OneHotEncodingSize translateObject(OneHotEncoding originalObject) {
        return translateToEstimate(originalObject);
    }
}
