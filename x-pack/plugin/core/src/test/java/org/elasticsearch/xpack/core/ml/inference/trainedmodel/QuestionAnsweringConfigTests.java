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
import java.util.function.Predicate;

public class QuestionAnsweringConfigTests extends InferenceConfigItemTestCase<QuestionAnsweringConfig> {

    public static QuestionAnsweringConfig mutateForVersion(QuestionAnsweringConfig instance, TransportVersion version) {
        return new QuestionAnsweringConfig(
            instance.getNumTopClasses(),
            instance.getMaxAnswerLength(),
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
    protected QuestionAnsweringConfig doParseInstance(XContentParser parser) throws IOException {
        return QuestionAnsweringConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<QuestionAnsweringConfig> instanceReader() {
        return QuestionAnsweringConfig::new;
    }

    @Override
    protected QuestionAnsweringConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected QuestionAnsweringConfig mutateInstance(QuestionAnsweringConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected QuestionAnsweringConfig mutateInstanceForVersion(QuestionAnsweringConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static QuestionAnsweringConfig createRandom() {
        return new QuestionAnsweringConfig(
            randomBoolean() ? null : randomIntBetween(0, 30),
            randomBoolean() ? null : randomIntBetween(1, 50),
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandomWithSpan(),
                    MPNetTokenizationTests.createRandomWithSpan(),
                    RobertaTokenizationTests.createRandomWithSpan()
                ),
            randomBoolean() ? null : randomAlphaOfLength(7)
        );
    }
}
