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
import java.util.List;
import java.util.function.Predicate;

public class ZeroShotClassificationConfigTests extends InferenceConfigItemTestCase<ZeroShotClassificationConfig> {

    public static ZeroShotClassificationConfig mutateForVersion(ZeroShotClassificationConfig instance, Version version) {
        return new ZeroShotClassificationConfig(
            instance.getClassificationLabels(),
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getHypothesisTemplate(),
            instance.isMultiLabel(),
            instance.getLabels().orElse(null),
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
    protected ZeroShotClassificationConfig doParseInstance(XContentParser parser) throws IOException {
        return ZeroShotClassificationConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<ZeroShotClassificationConfig> instanceReader() {
        return ZeroShotClassificationConfig::new;
    }

    @Override
    protected ZeroShotClassificationConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected ZeroShotClassificationConfig mutateInstanceForVersion(ZeroShotClassificationConfig instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static ZeroShotClassificationConfig createRandom() {
        return new ZeroShotClassificationConfig(
            randomFrom(List.of("entailment", "neutral", "contradiction"), List.of("contradiction", "neutral", "entailment")),
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandom(),
                    MPNetTokenizationTests.createRandom(),
                    RobertaTokenizationTests.createRandom()
                ),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomBoolean() ? null : randomList(1, 5, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomAlphaOfLength(7)
        );
    }
}
