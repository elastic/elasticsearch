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

public class FillMaskConfigTests extends AbstractBWCSerializationTestCase<FillMaskConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected FillMaskConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? FillMaskConfig.fromXContentLenient(parser) : FillMaskConfig.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<FillMaskConfig> instanceReader() {
        return FillMaskConfig::new;
    }

    @Override
    protected FillMaskConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected FillMaskConfig mutateInstanceForVersion(FillMaskConfig instance, Version version) {
        return instance;
    }

    public static FillMaskConfig createRandom() {
        return new FillMaskConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : TokenizationParamsTests.createRandom()
        );
    }
}
