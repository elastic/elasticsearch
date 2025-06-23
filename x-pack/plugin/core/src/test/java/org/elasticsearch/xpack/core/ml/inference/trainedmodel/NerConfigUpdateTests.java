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
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.equalTo;

public class NerConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<NerConfigUpdate> {

    public static NerConfigUpdate randomUpdate() {
        NerConfigUpdate.Builder builder = new NerConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static NerConfigUpdate mutateForVersion(NerConfigUpdate instance, TransportVersion version) {
        if (version.before(TransportVersions.V_8_1_0)) {
            return new NerConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    Tuple<Map<String, Object>, NerConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        NerConfigUpdate expected = new NerConfigUpdate("ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    NerConfigUpdate fromMap(Map<String, Object> map) {
        return NerConfigUpdate.fromMap(map);
    }

    public void testApply() {
        NerConfig originalConfig = NerConfigTests.createRandom();

        assertThat(originalConfig, equalTo(originalConfig.apply(new NerConfigUpdate.Builder().build())));

        assertThat(
            new NerConfig(
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getClassificationLabels(),
                "ml-results"
            ),
            equalTo(originalConfig.apply(new NerConfigUpdate.Builder().setResultsField("ml-results").build()))
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new NerConfig(
                originalConfig.getVocabularyConfig(),
                tokenization,
                originalConfig.getClassificationLabels(),
                originalConfig.getResultsField()
            ),
            equalTo(
                originalConfig.apply(
                    new NerConfigUpdate.Builder().setTokenizationUpdate(
                        createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                    ).build()
                )
            )
        );
    }

    @Override
    protected NerConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return NerConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<NerConfigUpdate> instanceReader() {
        return NerConfigUpdate::new;
    }

    @Override
    protected NerConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected NerConfigUpdate mutateInstance(NerConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected NerConfigUpdate mutateInstanceForVersion(NerConfigUpdate instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
