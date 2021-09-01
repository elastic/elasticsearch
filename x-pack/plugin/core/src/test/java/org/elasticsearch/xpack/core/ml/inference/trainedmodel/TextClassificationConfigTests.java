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

public class TextClassificationConfigTests extends InferenceConfigItemTestCase<TextClassificationConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TextClassificationConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? TextClassificationConfig.fromXContentLenient(parser) : TextClassificationConfig.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<TextClassificationConfig> instanceReader() {
        return TextClassificationConfig::new;
    }

    @Override
    protected TextClassificationConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextClassificationConfig mutateInstanceForVersion(TextClassificationConfig instance, Version version) {
        return instance;
    }

    public static TextClassificationConfig createRandom() {
        return new TextClassificationConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ?
                null :
                randomFrom(BertTokenizationTests.createRandom(), DistilBertTokenizationTests.createRandom()),
            randomBoolean() ? null : randomList(5, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomIntBetween(-1, 10)
        );
    }
}
