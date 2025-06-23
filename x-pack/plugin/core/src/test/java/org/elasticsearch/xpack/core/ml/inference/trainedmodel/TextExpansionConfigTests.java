/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;

public class TextExpansionConfigTests extends InferenceConfigItemTestCase<TextExpansionConfig> {

    public static TextExpansionConfig createRandom() {
        // create a tokenization config with a no span setting.
        var tokenization = new BertTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.FIRST),
            null
        );

        return new TextExpansionConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : tokenization,
            randomBoolean() ? null : randomAlphaOfLength(5)
        );
    }

    public static TextExpansionConfig mutateForVersion(TextExpansionConfig instance, TransportVersion version) {
        return new TextExpansionConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getResultsField()
        );
    }

    @Override
    protected Writeable.Reader<TextExpansionConfig> instanceReader() {
        return TextExpansionConfig::new;
    }

    @Override
    protected TextExpansionConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextExpansionConfig mutateInstance(TextExpansionConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextExpansionConfig doParseInstance(XContentParser parser) throws IOException {
        return TextExpansionConfig.fromXContentLenient(parser);
    }

    @Override
    protected TextExpansionConfig mutateInstanceForVersion(TextExpansionConfig instance, TransportVersion version) {
        return instance;
    }
}
