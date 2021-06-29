/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TargetMeanEncodingSizeTests extends SizeEstimatorTestCase<TargetMeanEncodingSize, TargetMeanEncoding> {

    static TargetMeanEncodingSize createRandom() {
        return new TargetMeanEncodingSize(randomInt(100),
            randomInt(100),
            Stream.generate(() -> randomIntBetween(5, 10))
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.toList()));
    }

    static TargetMeanEncodingSize translateToEstimate(TargetMeanEncoding encoding) {
        return new TargetMeanEncodingSize(encoding.getField().length(),
            encoding.getFeatureName().length(),
            encoding.getMeanMap().keySet().stream().map(String::length).collect(Collectors.toList()));
    }

    @Override
    protected TargetMeanEncodingSize createTestInstance() {
        return createRandom();
    }

    @Override
    protected TargetMeanEncodingSize doParseInstance(XContentParser parser) {
        return TargetMeanEncodingSize.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    TargetMeanEncoding generateTrueObject() {
        return TargetMeanEncodingTests.createRandom();
    }

    @Override
    TargetMeanEncodingSize translateObject(TargetMeanEncoding originalObject) {
        return translateToEstimate(originalObject);
    }
}
