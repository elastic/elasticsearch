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
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig.NUM_TOP_CLASSES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig.QUESTION;
import static org.hamcrest.Matchers.equalTo;

public class QuestionAnsweringConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<QuestionAnsweringConfigUpdate> {

    public static QuestionAnsweringConfigUpdate randomUpdate() {
        return new QuestionAnsweringConfigUpdate(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(0, 15),
            randomBoolean() ? null : randomIntBetween(1, 100),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomBoolean() ? null : new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null)
        );
    }

    public static QuestionAnsweringConfigUpdate mutateForVersion(QuestionAnsweringConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new QuestionAnsweringConfigUpdate(
                instance.getQuestion(),
                instance.getNumTopClasses(),
                instance.getMaxAnswerLength(),
                instance.getResultsField(),
                null
            );
        }
        return instance;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected QuestionAnsweringConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return QuestionAnsweringConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<QuestionAnsweringConfigUpdate> instanceReader() {
        return QuestionAnsweringConfigUpdate::new;
    }

    @Override
    protected QuestionAnsweringConfigUpdate createTestInstance() {
        return createRandom();
    }

    @Override
    protected QuestionAnsweringConfigUpdate mutateInstance(QuestionAnsweringConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected QuestionAnsweringConfigUpdate mutateInstanceForVersion(QuestionAnsweringConfigUpdate instance, Version version) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, QuestionAnsweringConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        QuestionAnsweringConfigUpdate expected = new QuestionAnsweringConfigUpdate(
            "What is the meaning of life?",
            3,
            20,
            "ml-results",
            expectedTokenization
        );

        Map<String, Object> config = new HashMap<>() {
            {
                put(QUESTION.getPreferredName(), "What is the meaning of life?");
                put(NUM_TOP_CLASSES.getPreferredName(), 3);
                put(QuestionAnsweringConfig.MAX_ANSWER_LENGTH.getPreferredName(), 20);
                put(QuestionAnsweringConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    QuestionAnsweringConfigUpdate fromMap(Map<String, Object> map) {
        return QuestionAnsweringConfigUpdate.fromMap(map);
    }

    public void testApply() {
        Tokenization tokenizationConfig = randomFrom(
            BertTokenizationTests.createRandom(),
            MPNetTokenizationTests.createRandom(),
            RobertaTokenizationTests.createRandom()
        );
        QuestionAnsweringConfig originalConfig = new QuestionAnsweringConfig(
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean() ? null : randomIntBetween(1, 20),
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            tokenizationConfig,
            randomBoolean() ? null : randomAlphaOfLength(8)
        );
        assertThat(
            new QuestionAnsweringConfig(
                "Are you my mother?",
                4,
                40,
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getResultsField()
            ),
            equalTo(
                new QuestionAnsweringConfigUpdate.Builder().setQuestion("Are you my mother?")
                    .setNumTopClasses(4)
                    .setMaxAnswerLength(40)
                    .build()
                    .apply(originalConfig)
            )
        );
        assertThat(
            new QuestionAnsweringConfig(
                "Are you my mother?",
                originalConfig.getNumTopClasses(),
                originalConfig.getMaxAnswerLength(),
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                "updated-field"
            ),
            equalTo(
                new QuestionAnsweringConfigUpdate.Builder().setQuestion("Are you my mother?")
                    .setResultsField("updated-field")
                    .build()
                    .apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new QuestionAnsweringConfig(
                "Are you my mother?",
                originalConfig.getNumTopClasses(),
                originalConfig.getMaxAnswerLength(),
                originalConfig.getVocabularyConfig(),
                tokenization,
                originalConfig.getResultsField()
            ),
            equalTo(
                new QuestionAnsweringConfigUpdate.Builder().setQuestion("Are you my mother?")
                    .setTokenizationUpdate(createTokenizationUpdate(originalConfig.getTokenization(), truncate, null))
                    .build()
                    .apply(originalConfig)
            )
        );
    }

    public static QuestionAnsweringConfigUpdate createRandom() {
        return randomUpdate();
    }
}
