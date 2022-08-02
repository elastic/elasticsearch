/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/89008")
public class TextSimilarityConfigTests extends InferenceConfigItemTestCase<TextSimilarityConfig> {

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
    protected TextSimilarityConfig mutateInstanceForVersion(TextSimilarityConfig instance, Version version) {
        return instance;
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
