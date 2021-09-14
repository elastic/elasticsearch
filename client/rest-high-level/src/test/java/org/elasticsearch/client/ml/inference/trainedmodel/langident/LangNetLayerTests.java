/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Stream;


public class LangNetLayerTests extends AbstractXContentTestCase<LangNetLayer> {

    @Override
    protected LangNetLayer doParseInstance(XContentParser parser) throws IOException {
        return LangNetLayer.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected LangNetLayer createTestInstance() {
        return createRandom();
    }

    public static LangNetLayer createRandom() {
        int numWeights = randomIntBetween(1, 1000);
        return new LangNetLayer(
            Stream.generate(ESTestCase::randomDouble).limit(numWeights).mapToDouble(Double::doubleValue).toArray(),
            numWeights,
            1,
            Stream.generate(ESTestCase::randomDouble).limit(numWeights).mapToDouble(Double::doubleValue).toArray());
    }

}
