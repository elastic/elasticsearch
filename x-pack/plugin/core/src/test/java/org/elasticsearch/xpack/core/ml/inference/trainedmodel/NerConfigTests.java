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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class NerConfigTests extends InferenceConfigItemTestCase<NerConfig> {

    public static NerConfig mutateForVersion(NerConfig instance, Version version) {
        return new NerConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getClassificationLabels(),
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
    protected NerConfig doParseInstance(XContentParser parser) throws IOException {
        return NerConfig.fromXContentLenient(parser);
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
    protected NerConfig mutateInstance(NerConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected NerConfig mutateInstanceForVersion(NerConfig instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static NerConfig createRandom() {
        Set<String> randomClassificationLabels = new HashSet<>(
            Stream.generate(() -> randomFrom("O", "B_PER", "I_PER", "B_ORG", "I_ORG", "B_LOC", "I_LOC", "B_CUSTOM", "I_CUSTOM"))
                .limit(10)
                .toList()
        );
        randomClassificationLabels.add("O");
        return new NerConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandom(),
                    MPNetTokenizationTests.createRandom(),
                    RobertaTokenizationTests.createRandom()
                ),
            randomBoolean() ? null : new ArrayList<>(randomClassificationLabels),
            randomBoolean() ? null : randomAlphaOfLength(5)
        );
    }
}
