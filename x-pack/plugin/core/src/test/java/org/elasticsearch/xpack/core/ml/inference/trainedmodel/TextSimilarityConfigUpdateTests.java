/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig.TEXT;
import static org.hamcrest.Matchers.equalTo;

public class TextSimilarityConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<TextSimilarityConfigUpdate> {

    public static TextSimilarityConfigUpdate randomUpdate() {
        return new TextSimilarityConfigUpdate(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomBoolean() ? null : new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null),
            randomBoolean()
                ? null
                : randomFrom(
                    Arrays.stream(TextSimilarityConfig.SpanScoreFunction.values())
                        .map(TextSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
    }

    public static TextSimilarityConfigUpdate mutateForVersion(TextSimilarityConfigUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_1_0)) {
            return new TextSimilarityConfigUpdate(instance.getText(), instance.getResultsField(), null, null);
        }
        return instance;
    }

    @Override
    protected TextSimilarityConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TextSimilarityConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<TextSimilarityConfigUpdate> instanceReader() {
        return TextSimilarityConfigUpdate::new;
    }

    @Override
    protected TextSimilarityConfigUpdate createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextSimilarityConfigUpdate mutateInstance(TextSimilarityConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextSimilarityConfigUpdate mutateInstanceForVersion(TextSimilarityConfigUpdate instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, TextSimilarityConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        String func = randomFrom(
            Arrays.stream(TextSimilarityConfig.SpanScoreFunction.values())
                .map(TextSimilarityConfig.SpanScoreFunction::toString)
                .toArray(String[]::new)
        );
        TextSimilarityConfigUpdate expected = new TextSimilarityConfigUpdate(
            "What is the meaning of life?",
            "ml-results",
            expectedTokenization,
            func
        );

        Map<String, Object> config = new HashMap<>() {
            {
                put(TEXT.getPreferredName(), "What is the meaning of life?");
                put(TextSimilarityConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                put(TextSimilarityConfig.SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName(), func);
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    TextSimilarityConfigUpdate fromMap(Map<String, Object> map) {
        return TextSimilarityConfigUpdate.fromMap(map);
    }

    public void testApply() {
        Tokenization tokenizationConfig = randomFrom(
            BertTokenizationTests.createRandom(),
            MPNetTokenizationTests.createRandom(),
            RobertaTokenizationTests.createRandom()
        );
        TextSimilarityConfig originalConfig = new TextSimilarityConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            tokenizationConfig,
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean()
                ? null
                : randomFrom(
                    Arrays.stream(TextSimilarityConfig.SpanScoreFunction.values())
                        .map(TextSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
        assertThat(
            new TextSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getResultsField(),
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(new TextSimilarityConfigUpdate.Builder().setText("Are you my mother?").build().apply(originalConfig))
        );
        assertThat(
            new TextSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                "updated-field",
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(
                new TextSimilarityConfigUpdate.Builder().setText("Are you my mother?")
                    .setResultsField("updated-field")
                    .build()
                    .apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new TextSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                tokenization,
                originalConfig.getResultsField(),
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(
                new TextSimilarityConfigUpdate.Builder().setText("Are you my mother?")
                    .setTokenizationUpdate(createTokenizationUpdate(originalConfig.getTokenization(), truncate, null))
                    .build()
                    .apply(originalConfig)
            )
        );
    }

    public static TextSimilarityConfigUpdate createRandom() {
        return randomUpdate();
    }
}
