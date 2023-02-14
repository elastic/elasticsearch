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
import java.util.Arrays;
import java.util.function.Predicate;

public class TextSimilarityConfigTests extends InferenceConfigItemTestCase<TextSimilarityConfig> {

    public static TextSimilarityConfig mutateForVersion(TextSimilarityConfig instance, Version version) {
        return new TextSimilarityConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getResultsField(),
            instance.getSpanScoreFunction().toString()
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
    protected TextSimilarityConfig doParseInstance(XContentParser parser) throws IOException {
        return TextSimilarityConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<TextSimilarityConfig> instanceReader() {
        return TextSimilarityConfig::new;
    }

    @Override
    protected TextSimilarityConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextSimilarityConfig mutateInstance(TextSimilarityConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextSimilarityConfig mutateInstanceForVersion(TextSimilarityConfig instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static TextSimilarityConfig createRandom() {
        return new TextSimilarityConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandomWithSpan(),
                    MPNetTokenizationTests.createRandomWithSpan(),
                    RobertaTokenizationTests.createRandomWithSpan()
                ),
            randomBoolean() ? null : randomAlphaOfLength(7),
            randomBoolean()
                ? null
                : randomFrom(
                    Arrays.stream(TextSimilarityConfig.SpanScoreFunction.values())
                        .map(TextSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
    }
}
