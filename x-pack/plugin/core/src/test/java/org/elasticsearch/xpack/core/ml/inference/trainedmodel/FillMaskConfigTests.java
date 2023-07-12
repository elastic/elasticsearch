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

public class FillMaskConfigTests extends InferenceConfigItemTestCase<FillMaskConfig> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public static FillMaskConfig mutateForVersion(FillMaskConfig instance, TransportVersion version) {
        return new FillMaskConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getNumTopClasses(),
            instance.getResultsField()
        );
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected FillMaskConfig doParseInstance(XContentParser parser) throws IOException {
        return FillMaskConfig.fromXContentLenient(parser);
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
    protected FillMaskConfig mutateInstance(FillMaskConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected FillMaskConfig mutateInstanceForVersion(FillMaskConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static FillMaskConfig createRandom() {
        return new FillMaskConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandom(),
                    MPNetTokenizationTests.createRandom(),
                    RobertaTokenizationTests.createRandom()
                ),
            randomBoolean() ? null : randomInt(),
            randomBoolean() ? null : randomAlphaOfLength(5)
        );
    }

    public void testCreateBuilder() {

        VocabularyConfig vocabularyConfig = randomBoolean() ? null : VocabularyConfigTests.createRandom();

        Tokenization tokenization = randomBoolean()
            ? null
            : randomFrom(
                BertTokenizationTests.createRandom(),
                MPNetTokenizationTests.createRandom(),
                RobertaTokenizationTests.createRandom()
            );

        Integer numTopClasses = randomBoolean() ? null : randomInt();

        String resultsField = randomBoolean() ? null : randomAlphaOfLength(5);

        new FillMaskConfig.Builder().setVocabularyConfig(vocabularyConfig)
            .setTokenization(tokenization)
            .setNumTopClasses(numTopClasses)
            .setResultsField(resultsField)
            .setMaskToken(tokenization == null ? null : tokenization.getMaskToken())
            .build();
    }

    public void testCreateBuilderWithException() throws Exception {

        VocabularyConfig vocabularyConfig = randomBoolean() ? null : VocabularyConfigTests.createRandom();

        Tokenization tokenization = randomBoolean()
            ? null
            : randomFrom(
                BertTokenizationTests.createRandom(),
                MPNetTokenizationTests.createRandom(),
                RobertaTokenizationTests.createRandom()
            );

        Integer numTopClasses = randomBoolean() ? null : randomInt();

        String resultsField = randomBoolean() ? null : randomAlphaOfLength(5);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            FillMaskConfig fmc = new FillMaskConfig.Builder().setVocabularyConfig(vocabularyConfig)
                .setTokenization(tokenization)
                .setNumTopClasses(numTopClasses)
                .setResultsField(resultsField)
                .setMaskToken("not a real mask token")
                .build();
        });

    }

    public void testCreateBuilderWithNullMaskToken() {

        VocabularyConfig vocabularyConfig = randomBoolean() ? null : VocabularyConfigTests.createRandom();

        Tokenization tokenization = randomBoolean()
            ? null
            : randomFrom(
                BertTokenizationTests.createRandom(),
                MPNetTokenizationTests.createRandom(),
                RobertaTokenizationTests.createRandom()
            );

        Integer numTopClasses = randomBoolean() ? null : randomInt();

        String resultsField = randomBoolean() ? null : randomAlphaOfLength(5);

        new FillMaskConfig.Builder().setVocabularyConfig(vocabularyConfig)
            .setTokenization(tokenization)
            .setNumTopClasses(numTopClasses)
            .setResultsField(resultsField)
            .build();
    }

}
