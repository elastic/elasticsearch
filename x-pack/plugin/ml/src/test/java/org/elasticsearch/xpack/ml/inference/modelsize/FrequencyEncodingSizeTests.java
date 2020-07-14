/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FrequencyEncodingSizeTests extends SizeEstimatorTestCase<FrequencyEncodingSize, FrequencyEncoding> {

    static FrequencyEncodingSize createRandom() {
        return new FrequencyEncodingSize(randomInt(100),
            randomInt(100),
            Stream.generate(() -> randomIntBetween(5, 10))
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.toList()));
    }

    static FrequencyEncodingSize translateToEstimate(FrequencyEncoding encoding) {
        return new FrequencyEncodingSize(encoding.getField().length(),
            encoding.getFeatureName().length(),
            encoding.getFrequencyMap().keySet().stream().map(String::length).collect(Collectors.toList()));
    }

    @Override
    protected FrequencyEncodingSize createTestInstance() {
        return createRandom();
    }

    @Override
    protected FrequencyEncodingSize doParseInstance(XContentParser parser) {
        return FrequencyEncodingSize.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    FrequencyEncoding generateTrueObject() {
        return FrequencyEncodingTests.createRandom();
    }

    @Override
    FrequencyEncodingSize translateObject(FrequencyEncoding originalObject) {
        return translateToEstimate(originalObject);
    }
}
