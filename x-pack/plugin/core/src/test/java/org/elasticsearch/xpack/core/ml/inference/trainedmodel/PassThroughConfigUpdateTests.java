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

public class PassThroughConfigUpdateTests extends AbstractBWCSerializationTestCase<PassThroughConfigUpdate> {

    public void testFromMap() {
        PassThroughConfigUpdate expected = new PassThroughConfigUpdate("ml-results");
        Map<String, Object> config = new HashMap<>(){{
            put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
        }};
        assertThat(PassThroughConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> PassThroughConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }


    public void testApply() {
        PassThroughConfig originalConfig = PassThroughConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new PassThroughConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(new PassThroughConfig(
                originalConfig.getVocabularyConfig(),
                originalConfig.getTokenization(),
                "ml-results"),
            equalTo(new PassThroughConfigUpdate.Builder()
                .setResultsField("ml-results")
                .build()
                .apply(originalConfig)
            ));
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
        PassThroughConfigUpdate.Builder builder = new PassThroughConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        return builder.build();
    }

    @Override
    protected PassThroughConfigUpdate mutateInstanceForVersion(PassThroughConfigUpdate instance, Version version) {
        return instance;
    }
}
