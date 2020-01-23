/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ClassificationConfigTests extends AbstractSerializingTestCase<ClassificationConfig> {

    public static ClassificationConfig randomClassificationConfig() {
        return new ClassificationConfig(randomBoolean() ? null : randomIntBetween(-1, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10)
            );
    }

    public void testFromMap() {
        ClassificationConfig expected = ClassificationConfig.EMPTY_PARAMS;
        assertThat(ClassificationConfig.fromMap(Collections.emptyMap()), equalTo(expected));

        expected = new ClassificationConfig(3, "foo", "bar");
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 3);
        configMap.put(ClassificationConfig.RESULTS_FIELD.getPreferredName(), "foo");
        configMap.put(ClassificationConfig.TOP_CLASSES_RESULTS_FIELD.getPreferredName(), "bar");
        assertThat(ClassificationConfig.fromMap(configMap), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> ClassificationConfig.fromMap(Collections.singletonMap("some_key", 1)));
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    @Override
    protected ClassificationConfig createTestInstance() {
        return randomClassificationConfig();
    }

    @Override
    protected Writeable.Reader<ClassificationConfig> instanceReader() {
        return ClassificationConfig::new;
    }

    @Override
    protected ClassificationConfig doParseInstance(XContentParser parser) throws IOException {
        return ClassificationConfig.fromXContent(parser);
    }
}
