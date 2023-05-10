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

public class FillMaskConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<FillMaskConfigUpdate> {

    public static FillMaskConfigUpdate randomUpdate() {
        FillMaskConfigUpdate.Builder builder = new FillMaskConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setNumTopClasses(randomIntBetween(1, 4));
        }
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static FillMaskConfigUpdate mutateForVersion(FillMaskConfigUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_1_0)) {
            return new FillMaskConfigUpdate(instance.getNumTopClasses(), instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    Tuple<Map<String, Object>, FillMaskConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        int topClasses = randomIntBetween(1, 10);
        FillMaskConfigUpdate expected = new FillMaskConfigUpdate(topClasses, "ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                put(NlpConfig.NUM_TOP_CLASSES.getPreferredName(), topClasses);
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    FillMaskConfigUpdate fromMap(Map<String, Object> map) {
        return FillMaskConfigUpdate.fromMap(map);
    }

    public void testIsNoop() {
        assertTrue(new FillMaskConfigUpdate.Builder().build().isNoop(FillMaskConfigTests.createRandom()));

        assertFalse(
            new FillMaskConfigUpdate.Builder().setResultsField("foo")
                .build()
                .isNoop(new FillMaskConfig.Builder().setResultsField("bar").build())
        );

        assertFalse(
            new FillMaskConfigUpdate.Builder().setTokenizationUpdate(new BertTokenizationUpdate(Tokenization.Truncate.SECOND, null))
                .build()
                .isNoop(new FillMaskConfig.Builder().setResultsField("bar").build())
        );

        assertTrue(
            new FillMaskConfigUpdate.Builder().setNumTopClasses(3).build().isNoop(new FillMaskConfig.Builder().setNumTopClasses(3).build())
        );
    }

    public void testApply() {
        FillMaskConfig originalConfig = FillMaskConfigTests.createRandom();

        assertThat(originalConfig, equalTo(new FillMaskConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new FillMaskConfig.Builder(originalConfig).setResultsField("ml-results").build(),
            equalTo(new FillMaskConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );
        assertThat(
            new FillMaskConfig.Builder(originalConfig).setNumTopClasses(originalConfig.getNumTopClasses() + 1).build(),
            equalTo(
                new FillMaskConfigUpdate.Builder().setNumTopClasses(originalConfig.getNumTopClasses() + 1).build().apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new FillMaskConfig.Builder(originalConfig).setTokenization(tokenization).build(),
            equalTo(
                new FillMaskConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                ).build().apply(originalConfig)
            )
        );
    }

    @Override
    protected FillMaskConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return FillMaskConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<FillMaskConfigUpdate> instanceReader() {
        return FillMaskConfigUpdate::new;
    }

    @Override
    protected FillMaskConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected FillMaskConfigUpdate mutateInstance(FillMaskConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected FillMaskConfigUpdate mutateInstanceForVersion(FillMaskConfigUpdate instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
