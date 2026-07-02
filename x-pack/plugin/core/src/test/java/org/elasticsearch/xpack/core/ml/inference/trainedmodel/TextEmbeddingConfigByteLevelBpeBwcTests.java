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
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.test.BWCVersions.getAllBWCVersions;

/**
 * Wire BWC serialization tests for {@link TextEmbeddingConfig} when {@link ByteLevelBpeTokenization} is used.
 * {@link TextEmbeddingConfigTests} keeps the full historical transport versions for other tokenizations;
 * byte-level BPE requires {@link ByteLevelBpeTokenization#ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED}.
 */
public class TextEmbeddingConfigByteLevelBpeBwcTests extends InferenceConfigItemTestCase<TextEmbeddingConfig> {

    @Override
    protected List<TransportVersion> bwcVersions() {
        return getAllBWCVersions().stream()
            .filter(v -> v.supports(ByteLevelBpeTokenization.ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED))
            .collect(Collectors.toList());
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
        return TextEmbeddingConfig.create(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            ByteLevelBpeTokenizationTests.createRandom(),
            randomBoolean() ? null : randomAlphaOfLength(7),
            randomBoolean() ? null : randomIntBetween(1, 1000)
        );
    }

    @Override
    protected TextEmbeddingConfig mutateInstance(TextEmbeddingConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextEmbeddingConfig mutateInstanceForVersion(TextEmbeddingConfig instance, TransportVersion version) {
        return TextEmbeddingConfigTests.mutateForVersion(instance, version);
    }
}
