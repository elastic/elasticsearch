/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class WeightedModeTests extends AbstractXContentTestCase<WeightedMode> {

    WeightedMode createTestInstance(int numberOfWeights) {
        return new WeightedMode(
            randomIntBetween(2, 10),
            Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).collect(Collectors.toList()));
    }

    @Override
    protected WeightedMode doParseInstance(XContentParser parser) throws IOException {
        return WeightedMode.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected WeightedMode createTestInstance() {
        return randomBoolean() ? new WeightedMode(randomIntBetween(2, 10), null) : createTestInstance(randomIntBetween(1, 100));
    }

}
