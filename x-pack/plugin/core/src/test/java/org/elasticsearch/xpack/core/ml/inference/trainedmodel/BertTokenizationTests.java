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

public class BertTokenizationTests extends AbstractBWCSerializationTestCase<BertTokenization> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected BertTokenization doParseInstance(XContentParser parser) throws IOException {
        return BertTokenization.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<BertTokenization> instanceReader() {
        return BertTokenization::new;
    }

    @Override
    protected BertTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected BertTokenization mutateInstanceForVersion(BertTokenization instance, Version version) {
        return instance;
    }

    public static BertTokenization createRandom() {
        return new BertTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024)
        );
    }
}
