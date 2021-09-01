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
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;
import org.junit.Before;

import java.io.IOException;

public class NerConfigTests extends InferenceConfigItemTestCase<NerConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected NerConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? NerConfig.fromXContentLenient(parser) : NerConfig.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<NerConfig> instanceReader() {
        return NerConfig::new;
    }

    @Override
    protected NerConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected NerConfig mutateInstanceForVersion(NerConfig instance, Version version) {
        return instance;
    }

    public static NerConfig createRandom() {
        return new NerConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ?
                null :
                randomFrom(BertTokenizationTests.createRandom(), DistilBertTokenizationTests.createRandom()),
            randomBoolean() ? null : randomList(5, () -> randomAlphaOfLength(10))
        );
    }
}
