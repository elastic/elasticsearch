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

public class SentimentAnalysisConfigTests extends InferenceConfigItemTestCase<SentimentAnalysisConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected SentimentAnalysisConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? SentimentAnalysisConfig.fromXContentLenient(parser) : SentimentAnalysisConfig.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<SentimentAnalysisConfig> instanceReader() {
        return SentimentAnalysisConfig::new;
    }

    @Override
    protected SentimentAnalysisConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected SentimentAnalysisConfig mutateInstanceForVersion(SentimentAnalysisConfig instance, Version version) {
        return instance;
    }

    public static SentimentAnalysisConfig createRandom() {
        return new SentimentAnalysisConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ?
                null :
                randomFrom(BertTokenizationParamsTests.createRandom(), DistilBertTokenizationParamsTests.createRandom()),
            randomBoolean() ? null : randomList(5, () -> randomAlphaOfLength(10))
        );
    }
}
