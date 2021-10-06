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

public class TextEmbeddingConfigUpdateTests extends AbstractBWCSerializationTestCase<TextEmbeddingConfigUpdate> {

    public void testFromMap() {
        TextEmbeddingConfigUpdate expected = new TextEmbeddingConfigUpdate("ml-results");
        Map<String, Object> config = new HashMap<>(){{
            put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
        }};
        assertThat(TextEmbeddingConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TextEmbeddingConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }


    public void testApply() {
        TextEmbeddingConfig originalConfig = TextEmbeddingConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new TextEmbeddingConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(new TextEmbeddingConfig(
            originalConfig.getVocabularyConfig(),
            originalConfig.getTokenization(),
            "ml-results"),
            equalTo(new TextEmbeddingConfigUpdate.Builder()
                .setResultsField("ml-results")
                .build()
                .apply(originalConfig)
            ));
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
        TextEmbeddingConfigUpdate.Builder builder = new TextEmbeddingConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        return builder.build();
    }

    @Override
    protected TextEmbeddingConfigUpdate mutateInstanceForVersion(TextEmbeddingConfigUpdate instance, Version version) {
        return instance;
    }
}
