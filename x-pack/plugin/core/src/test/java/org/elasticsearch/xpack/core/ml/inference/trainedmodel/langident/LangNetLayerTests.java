/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.stream.Stream;


public class LangNetLayerTests extends AbstractSerializingTestCase<LangNetLayer> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected LangNetLayer doParseInstance(XContentParser parser) throws IOException {
        return lenient ? LangNetLayer.LENIENT_PARSER.apply(parser, null) : LangNetLayer.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
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

    @Override
    protected Writeable.Reader<LangNetLayer> instanceReader() {
        return LangNetLayer::new;
    }

}
