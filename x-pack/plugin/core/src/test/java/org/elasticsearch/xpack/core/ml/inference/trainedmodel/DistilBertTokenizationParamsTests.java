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

public class DistilBertTokenizationParamsTests extends AbstractBWCSerializationTestCase<DistilBertTokenizationParams> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected DistilBertTokenizationParams doParseInstance(XContentParser parser) throws IOException {
        return DistilBertTokenizationParams.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<DistilBertTokenizationParams> instanceReader() {
        return DistilBertTokenizationParams::new;
    }

    @Override
    protected DistilBertTokenizationParams createTestInstance() {
        return createRandom();
    }

    @Override
    protected DistilBertTokenizationParams mutateInstanceForVersion(DistilBertTokenizationParams instance, Version version) {
        return instance;
    }

    public static DistilBertTokenizationParams createRandom() {
        return new DistilBertTokenizationParams(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024)
        );
    }
}
