/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class TextEmbeddingConfigTests extends InferenceConfigItemTestCase<TextEmbeddingConfig> {

    public static TextEmbeddingConfig mutateForVersion(TextEmbeddingConfig instance, Version version) {
        return new TextEmbeddingConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getResultsField()
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TextEmbeddingConfig doParseInstance(XContentParser parser) throws IOException {
        return TextEmbeddingConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<TextEmbeddingConfig> instanceReader() {
        return TextEmbeddingConfig::new;
    }

    @Override
    protected TextEmbeddingConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextEmbeddingConfig mutateInstance(TextEmbeddingConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextEmbeddingConfig mutateInstanceForVersion(TextEmbeddingConfig instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static TextEmbeddingConfig createRandom() {
        return new TextEmbeddingConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandom(),
                    MPNetTokenizationTests.createRandom(),
                    RobertaTokenizationTests.createRandom()
                ),
            randomBoolean() ? null : randomAlphaOfLength(7)
        );
    }
}
