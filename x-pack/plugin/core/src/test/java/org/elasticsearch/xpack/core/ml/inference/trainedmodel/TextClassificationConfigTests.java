/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;

public class TextClassificationConfigTests extends InferenceConfigItemTestCase<TextClassificationConfig> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TextClassificationConfig doParseInstance(XContentParser parser) throws IOException {
        return TextClassificationConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<TextClassificationConfig> instanceReader() {
        return TextClassificationConfig::new;
    }

    @Override
    protected TextClassificationConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected TextClassificationConfig mutateInstanceForVersion(TextClassificationConfig instance, Version version) {
        return instance;
    }

    public void testInvalidClassificationLabels() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new TextClassificationConfig(null, null, null, null, null)
        );

        assertThat(e.getMessage(), containsString("[text_classification] requires at least 2 [classification_labels]; provided null"));

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new TextClassificationConfig(null, null, List.of("too-few"), null, null)
        );
        assertThat(e.getMessage(), containsString("[text_classification] requires at least 2 [classification_labels]; provided [too-few]"));
    }

    public static TextClassificationConfig createRandom() {
        return new TextClassificationConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : randomFrom(BertTokenizationTests.createRandom(), MPNetTokenizationTests.createRandom()),
            randomList(2, 5, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomBoolean() ? -1 : randomIntBetween(1, 10),
            randomBoolean() ? null : randomAlphaOfLength(6)
        );
    }
}
