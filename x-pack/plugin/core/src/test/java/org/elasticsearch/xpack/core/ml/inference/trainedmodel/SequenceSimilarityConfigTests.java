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

public class SequenceSimilarityConfigTests extends InferenceConfigItemTestCase<SequenceSimilarityConfig> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected SequenceSimilarityConfig doParseInstance(XContentParser parser) throws IOException {
        return SequenceSimilarityConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<SequenceSimilarityConfig> instanceReader() {
        return SequenceSimilarityConfig::new;
    }

    @Override
    protected SequenceSimilarityConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected SequenceSimilarityConfig mutateInstanceForVersion(SequenceSimilarityConfig instance, Version version) {
        return instance;
    }

    public static SequenceSimilarityConfig createRandom() {
        return new SequenceSimilarityConfig(
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
                    Arrays.stream(SequenceSimilarityConfig.SpanScoreFunction.values())
                        .map(SequenceSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
    }
}
