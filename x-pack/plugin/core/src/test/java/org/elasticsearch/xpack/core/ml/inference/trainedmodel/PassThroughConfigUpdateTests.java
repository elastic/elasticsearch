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
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PassThroughConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<PassThroughConfigUpdate> {

    public static PassThroughConfigUpdate randomUpdate() {
        PassThroughConfigUpdate.Builder builder = new PassThroughConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static PassThroughConfigUpdate mutateForVersion(PassThroughConfigUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_1_0)) {
            return new PassThroughConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    Tuple<Map<String, Object>, PassThroughConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        PassThroughConfigUpdate expected = new PassThroughConfigUpdate("ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    PassThroughConfigUpdate fromMap(Map<String, Object> map) {
        return PassThroughConfigUpdate.fromMap(map);
    }

    public void testApply() {
        PassThroughConfig originalConfig = PassThroughConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new PassThroughConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new PassThroughConfig(originalConfig.getVocabularyConfig(), originalConfig.getTokenization(), "ml-results"),
            equalTo(new PassThroughConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new PassThroughConfig(originalConfig.getVocabularyConfig(), tokenization, originalConfig.getResultsField()),
            equalTo(
                new PassThroughConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                ).build().apply(originalConfig)
            )
        );
    }

    @Override
    protected PassThroughConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return PassThroughConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<PassThroughConfigUpdate> instanceReader() {
        return PassThroughConfigUpdate::new;
    }

    @Override
    protected PassThroughConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected PassThroughConfigUpdate mutateInstance(PassThroughConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected PassThroughConfigUpdate mutateInstanceForVersion(PassThroughConfigUpdate instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
