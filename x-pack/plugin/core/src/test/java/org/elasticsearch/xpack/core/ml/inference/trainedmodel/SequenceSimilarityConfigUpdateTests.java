/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.SequenceSimilarityConfig.SEQUENCE;
import static org.hamcrest.Matchers.equalTo;

public class SequenceSimilarityConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<SequenceSimilarityConfigUpdate> {

    public static SequenceSimilarityConfigUpdate randomUpdate() {
        return new SequenceSimilarityConfigUpdate(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomBoolean() ? null : new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null),
            randomBoolean()
                ? null
                : randomFrom(
                    Arrays.stream(SequenceSimilarityConfig.SpanScoreFunction.values())
                        .map(SequenceSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
    }

    public static SequenceSimilarityConfigUpdate mutateForVersion(SequenceSimilarityConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new SequenceSimilarityConfigUpdate(instance.getSequence(), instance.getResultsField(), null, null);
        }
        return instance;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected SequenceSimilarityConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return SequenceSimilarityConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<SequenceSimilarityConfigUpdate> instanceReader() {
        return SequenceSimilarityConfigUpdate::new;
    }

    @Override
    protected SequenceSimilarityConfigUpdate createTestInstance() {
        return createRandom();
    }

    @Override
    protected SequenceSimilarityConfigUpdate mutateInstanceForVersion(SequenceSimilarityConfigUpdate instance, Version version) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, SequenceSimilarityConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        String func = randomFrom(
            Arrays.stream(SequenceSimilarityConfig.SpanScoreFunction.values())
                .map(SequenceSimilarityConfig.SpanScoreFunction::toString)
                .toArray(String[]::new)
        );
        SequenceSimilarityConfigUpdate expected = new SequenceSimilarityConfigUpdate(
            "What is the meaning of life?",
            "ml-results",
            expectedTokenization,
            func
        );

        Map<String, Object> config = new HashMap<>() {
            {
                put(SEQUENCE.getPreferredName(), "What is the meaning of life?");
                put(SequenceSimilarityConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                put(SequenceSimilarityConfig.SPAN_SCORE_COMBINATION_FUNCTION.getPreferredName(), func);
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    SequenceSimilarityConfigUpdate fromMap(Map<String, Object> map) {
        return SequenceSimilarityConfigUpdate.fromMap(map);
    }

    public void testApply() {
        Tokenization tokenizationConfig = randomFrom(
            BertTokenizationTests.createRandom(),
            MPNetTokenizationTests.createRandom(),
            RobertaTokenizationTests.createRandom()
        );
        SequenceSimilarityConfig originalConfig = new SequenceSimilarityConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            tokenizationConfig,
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean()
                ? null
                : randomFrom(
                    Arrays.stream(SequenceSimilarityConfig.SpanScoreFunction.values())
                        .map(SequenceSimilarityConfig.SpanScoreFunction::toString)
                        .toArray(String[]::new)
                )
        );
        assertThat(
            new SequenceSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getResultsField(),
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(new SequenceSimilarityConfigUpdate.Builder().setSequence("Are you my mother?").build().apply(originalConfig))
        );
        assertThat(
            new SequenceSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                "updated-field",
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(
                new SequenceSimilarityConfigUpdate.Builder().setSequence("Are you my mother?")
                    .setResultsField("updated-field")
                    .build()
                    .apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new SequenceSimilarityConfig(
                "Are you my mother?",
                originalConfig.getVocabularyConfig(),
                tokenization,
                originalConfig.getResultsField(),
                originalConfig.getSpanScoreFunction()
            ),
            equalTo(
                new SequenceSimilarityConfigUpdate.Builder().setSequence("Are you my mother?")
                    .setTokenizationUpdate(createTokenizationUpdate(originalConfig.getTokenization(), truncate, null))
                    .build()
                    .apply(originalConfig)
            )
        );
    }

    public static SequenceSimilarityConfigUpdate createRandom() {
        return randomUpdate();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }
}
