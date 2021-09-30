/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class NerConfigUpdateTests extends AbstractBWCSerializationTestCase<NerConfigUpdate> {

    public void testFromMap() {
        NerConfigUpdate expected = new NerConfigUpdate("ml-results");
        Map<String, Object> config = new HashMap<>(){{
            put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
        }};
        assertThat(NerConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> NerConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }


    public void testApply() {
        NerConfig originalConfig = NerConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new NerConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(new NerConfig(
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                originalConfig.getClassificationLabels(),
                "ml-results"),
            equalTo(new NerConfigUpdate.Builder()
                .setResultsField("ml-results")
                .build()
                .apply(originalConfig)
            ));
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
        NerConfigUpdate.Builder builder = new NerConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        return builder.build();
    }

    @Override
    protected NerConfigUpdate mutateInstanceForVersion(NerConfigUpdate instance, Version version) {
        return instance;
    }
}

