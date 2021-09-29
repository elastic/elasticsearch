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

public class FillMaskConfigUpdateTests extends AbstractBWCSerializationTestCase<FillMaskConfigUpdate> {

    public void testFromMap() {
        FillMaskConfigUpdate expected = new FillMaskConfigUpdate(3, "ml-results");
        Map<String, Object> config = new HashMap<>(){{
            put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            put(NlpConfig.NUM_TOP_CLASSES.getPreferredName(), 3);
        }};
        assertThat(FillMaskConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> FillMaskConfigUpdate.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testIsNoop() {
        assertTrue(new FillMaskConfigUpdate.Builder().build().isNoop(FillMaskConfigTests.createRandom()));

        assertFalse(new FillMaskConfigUpdate.Builder()
            .setResultsField("foo")
            .build()
            .isNoop(new FillMaskConfig.Builder().setResultsField("bar").build()));

        assertTrue(new FillMaskConfigUpdate.Builder()
            .setNumTopClasses(3)
            .build()
            .isNoop(new FillMaskConfig.Builder().setNumTopClasses(3).build()));
    }

    public void testApply() {
        FillMaskConfig originalConfig = FillMaskConfigTests.createRandom();

        assertThat(originalConfig, equalTo(new FillMaskConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(new FillMaskConfig.Builder(originalConfig)
                .setResultsField("ml-results")
                .build(),
            equalTo(new FillMaskConfigUpdate.Builder()
                .setResultsField("ml-results")
                .build()
                .apply(originalConfig)
            ));
        assertThat(new FillMaskConfig.Builder(originalConfig)
                .setNumTopClasses(originalConfig.getNumTopClasses() +1)
                .build(),
            equalTo(new FillMaskConfigUpdate.Builder()
                .setNumTopClasses(originalConfig.getNumTopClasses() +1)
                .build()
                .apply(originalConfig)
            ));
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
        FillMaskConfigUpdate.Builder builder = new FillMaskConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setNumTopClasses(randomIntBetween(1, 4));
        }
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        return builder.build();
    }

    @Override
    protected FillMaskConfigUpdate mutateInstanceForVersion(FillMaskConfigUpdate instance, Version version) {
        return instance;
    }
}
