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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TextClassificationConfigUpdateTests extends AbstractBWCSerializationTestCase<TextClassificationConfigUpdate> {

    public void testFromMap() {
        TextClassificationConfigUpdate expected = new TextClassificationConfigUpdate(List.of("foo", "bar"), 3, "ml-results");
        Map<String, Object> config = new HashMap<>(){{
            put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            put(NlpConfig.CLASSIFICATION_LABELS.getPreferredName(), List.of("foo", "bar"));
            put(NlpConfig.NUM_TOP_CLASSES.getPreferredName(), 3);
        }};
        assertThat(TextClassificationConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TextClassificationConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testIsNoop() {
        assertTrue(new TextClassificationConfigUpdate.Builder().build().isNoop(TextClassificationConfigTests.createRandom()));

        assertFalse(new TextClassificationConfigUpdate.Builder()
            .setResultsField("foo")
            .build()
            .isNoop(new TextClassificationConfig.Builder().setResultsField("bar").build()));

        assertTrue(new TextClassificationConfigUpdate.Builder()
            .setNumTopClasses(3)
            .build()
            .isNoop(new TextClassificationConfig.Builder().setNumTopClasses(3).build()));
        assertFalse(new TextClassificationConfigUpdate.Builder()
            .setClassificationLabels(List.of("a", "b"))
            .build()
            .isNoop(new TextClassificationConfig.Builder().setClassificationLabels(List.of("c")).build()));
    }

    public void testApply() {
        TextClassificationConfig originalConfig = TextClassificationConfigTests.createRandom();

        assertThat(originalConfig, equalTo(new TextClassificationConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(new TextClassificationConfig.Builder(originalConfig)
            .setClassificationLabels(List.of("foo", "bar"))
            .build(),
            equalTo(new TextClassificationConfigUpdate.Builder()
                .setClassificationLabels(List.of("foo", "bar"))
                .build()
                .apply(originalConfig)));
        assertThat(new TextClassificationConfig.Builder(originalConfig)
                .setResultsField("ml-results")
                .build(),
            equalTo(new TextClassificationConfigUpdate.Builder()
                .setResultsField("ml-results")
                .build()
                .apply(originalConfig)
            ));
        assertThat(new TextClassificationConfig.Builder(originalConfig)
                .setNumTopClasses(originalConfig.getNumTopClasses() +1)
                .build(),
            equalTo(new TextClassificationConfigUpdate.Builder()
                .setNumTopClasses(originalConfig.getNumTopClasses() +1)
                .build()
                .apply(originalConfig)
            ));
    }

    @Override
    protected TextClassificationConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TextClassificationConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<TextClassificationConfigUpdate> instanceReader() {
        return TextClassificationConfigUpdate::new;
    }

    @Override
    protected TextClassificationConfigUpdate createTestInstance() {
        TextClassificationConfigUpdate.Builder builder = new TextClassificationConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setNumTopClasses(randomIntBetween(1, 4));
        }
        if (randomBoolean()) {
            builder.setClassificationLabels(randomList(1, 3, () -> randomAlphaOfLength(4)));
        }
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        return builder.build();
    }

    @Override
    protected TextClassificationConfigUpdate mutateInstanceForVersion(TextClassificationConfigUpdate instance, Version version) {
        return instance;
    }
}
