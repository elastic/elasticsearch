/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class TextEmbeddingConfigTests extends InferenceConfigItemTestCase<TextEmbeddingConfig> {

    public static TextEmbeddingConfig mutateForVersion(TextEmbeddingConfig instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_8_0)) {
            return new TextEmbeddingConfig(
                instance.getVocabularyConfig(),
                InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
                instance.getResultsField(),
                null
            );
        } else {
            return new TextEmbeddingConfig(
                instance.getVocabularyConfig(),
                InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
                instance.getResultsField(),
                instance.getEmbeddingSize()
            );
        }
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
    protected TextEmbeddingConfig mutateInstanceForVersion(TextEmbeddingConfig instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public void testInvariants() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new TextEmbeddingConfig(null, BertTokenizationTests.createRandom(), null, 0)
        );
        assertEquals("[embedding_size] must be a number greater than 0; configured size [0]", e.getMessage());

        var invalidTokenization = new BertTokenization(true, true, 512, Tokenization.Truncate.NONE, 128);
        e = expectThrows(ElasticsearchStatusException.class, () -> new TextEmbeddingConfig(null, invalidTokenization, null, 200));
        assertEquals("[text_embedding] does not support windowing long text sequences; configured span [128]", e.getMessage());
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
            randomBoolean() ? null : randomAlphaOfLength(7),
            randomBoolean() ? null : randomIntBetween(1, 1000)
        );
    }
}
