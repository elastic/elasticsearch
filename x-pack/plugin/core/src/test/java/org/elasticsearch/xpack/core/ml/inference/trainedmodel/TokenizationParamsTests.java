/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class TokenizationParamsTests extends AbstractBWCSerializationTestCase<TokenizationParams> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TokenizationParams doParseInstance(XContentParser parser) throws IOException {
        return TokenizationParams.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<TokenizationParams> instanceReader() {
        return TokenizationParams::new;
    }

    @Override
    protected TokenizationParams createTestInstance() {
        return createRandom();
    }

    @Override
    protected TokenizationParams mutateInstanceForVersion(TokenizationParams instance, Version version) {
        return instance;
    }

    public static TokenizationParams createRandom() {
        return new TokenizationParams(randomBoolean(), randomBoolean(), randomIntBetween(1, 1024));
    }
}
