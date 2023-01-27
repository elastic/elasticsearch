/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class TextEmbeddingConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<TextEmbeddingConfigUpdate> {

    public static TextEmbeddingConfigUpdate randomUpdate() {
        TextEmbeddingConfigUpdate.Builder builder = new TextEmbeddingConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static TextEmbeddingConfigUpdate mutateForVersion(TextEmbeddingConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new TextEmbeddingConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    Tuple<Map<String, Object>, TextEmbeddingConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        TextEmbeddingConfigUpdate expected = new TextEmbeddingConfigUpdate("ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    TextEmbeddingConfigUpdate fromMap(Map<String, Object> map) {
        return TextEmbeddingConfigUpdate.fromMap(map);
    }

    public void testApply() {
        TextEmbeddingConfig originalConfig = TextEmbeddingConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new TextEmbeddingConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new TextEmbeddingConfig(originalConfig.getVocabularyConfig(), originalConfig.getTokenization(), "ml-results"),
            equalTo(new TextEmbeddingConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new TextEmbeddingConfig(originalConfig.getVocabularyConfig(), tokenization, originalConfig.getResultsField()),
            equalTo(
                new TextEmbeddingConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                ).build().apply(originalConfig)
            )
        );
    }

    @Override
    protected TextEmbeddingConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TextEmbeddingConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<TextEmbeddingConfigUpdate> instanceReader() {
        return TextEmbeddingConfigUpdate::new;
    }

    @Override
    protected TextEmbeddingConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected TextEmbeddingConfigUpdate mutateInstance(TextEmbeddingConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextEmbeddingConfigUpdate mutateInstanceForVersion(TextEmbeddingConfigUpdate instance, Version version) {
        return mutateForVersion(instance, version);
    }
}
