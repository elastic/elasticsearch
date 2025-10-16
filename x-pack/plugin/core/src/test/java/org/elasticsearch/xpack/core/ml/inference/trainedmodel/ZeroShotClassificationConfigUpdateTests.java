/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ZeroShotClassificationConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<ZeroShotClassificationConfigUpdate> {

    public static ZeroShotClassificationConfigUpdate randomUpdate() {
        return new ZeroShotClassificationConfigUpdate(
            randomBoolean() ? null : randomList(1, 5, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomBoolean() ? null : new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null)
        );
    }

    public static ZeroShotClassificationConfigUpdate mutateForVersion(
        ZeroShotClassificationConfigUpdate instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersions.V_8_1_0)) {
            return new ZeroShotClassificationConfigUpdate(instance.getLabels(), instance.getMultiLabel(), instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    protected ZeroShotClassificationConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return ZeroShotClassificationConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<ZeroShotClassificationConfigUpdate> instanceReader() {
        return ZeroShotClassificationConfigUpdate::new;
    }

    @Override
    protected ZeroShotClassificationConfigUpdate createTestInstance() {
        return createRandom();
    }

    @Override
    protected ZeroShotClassificationConfigUpdate mutateInstance(ZeroShotClassificationConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected ZeroShotClassificationConfigUpdate mutateInstanceForVersion(
        ZeroShotClassificationConfigUpdate instance,
        TransportVersion version
    ) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, ZeroShotClassificationConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        boolean multiLabel = randomBoolean();
        ZeroShotClassificationConfigUpdate expected = new ZeroShotClassificationConfigUpdate(
            List.of("foo", "bar"),
            multiLabel,
            "ml-results",
            expectedTokenization
        );

        Map<String, Object> config = new HashMap<>() {
            {
                put(ZeroShotClassificationConfig.LABELS.getPreferredName(), List.of("foo", "bar"));
                put(ZeroShotClassificationConfig.MULTI_LABEL.getPreferredName(), multiLabel);
                put(ZeroShotClassificationConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    ZeroShotClassificationConfigUpdate fromMap(Map<String, Object> map) {
        return ZeroShotClassificationConfigUpdate.fromMap(map);
    }

    public void testApply() {
        ZeroShotClassificationConfig originalConfig = new ZeroShotClassificationConfig(
            randomFrom(List.of("entailment", "neutral", "contradiction"), List.of("contradiction", "neutral", "entailment")),
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : BertTokenizationTests.createRandom(),
            randomAlphaOfLength(10),
            randomBoolean(),
            randomList(1, 5, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomAlphaOfLength(8)
        );

        assertThat(originalConfig, equalTo(originalConfig.apply(new ZeroShotClassificationConfigUpdate.Builder().build())));

        assertThat(
            new ZeroShotClassificationConfig(
                originalConfig.getClassificationLabels(),
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getHypothesisTemplate(),
                originalConfig.isMultiLabel(),
                List.of("foo", "bar"),
                originalConfig.getResultsField()
            ),
            equalTo(originalConfig.apply(new ZeroShotClassificationConfigUpdate.Builder().setLabels(List.of("foo", "bar")).build()))
        );
        assertThat(
            new ZeroShotClassificationConfig(
                originalConfig.getClassificationLabels(),
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getHypothesisTemplate(),
                true,
                originalConfig.getLabels().orElse(null),
                originalConfig.getResultsField()
            ),
            equalTo(originalConfig.apply(new ZeroShotClassificationConfigUpdate.Builder().setMultiLabel(true).build()))
        );
        assertThat(
            new ZeroShotClassificationConfig(
                originalConfig.getClassificationLabels(),
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getHypothesisTemplate(),
                originalConfig.isMultiLabel(),
                originalConfig.getLabels().orElse(null),
                "updated-field"
            ),
            equalTo(originalConfig.apply(new ZeroShotClassificationConfigUpdate.Builder().setResultsField("updated-field").build()))
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new ZeroShotClassificationConfig(
                originalConfig.getClassificationLabels(),
                originalConfig.getVocabularyConfig(),
                tokenization,
                originalConfig.getHypothesisTemplate(),
                originalConfig.isMultiLabel(),
                originalConfig.getLabels().orElse(null),
                originalConfig.getResultsField()
            ),
            equalTo(
                originalConfig.apply(
                    new ZeroShotClassificationConfigUpdate.Builder().setTokenizationUpdate(
                        createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                    ).build()
                )
            )
        );
    }

    public void testApplyWithEmptyLabelsInConfigAndUpdate() {
        ZeroShotClassificationConfig originalConfig = new ZeroShotClassificationConfig(
            randomFrom(List.of("entailment", "neutral", "contradiction"), List.of("contradiction", "neutral", "entailment")),
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : BertTokenizationTests.createRandom(),
            randomAlphaOfLength(10),
            randomBoolean(),
            null,
            null
        );

        Exception ex = expectThrows(Exception.class, () -> originalConfig.apply(new ZeroShotClassificationConfigUpdate.Builder().build()));
        assertThat(
            ex.getMessage(),
            containsString("stored configuration has no [labels] defined, supplied inference_config update must supply [labels]")
        );
    }

    public static ZeroShotClassificationConfigUpdate createRandom() {
        return randomUpdate();
    }
}
