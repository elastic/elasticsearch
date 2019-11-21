/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class ClassificationConfigTests extends AbstractWireSerializingTestCase<ClassificationConfig> {

    public static ClassificationConfig randomClassificationConfig() {
        return new ClassificationConfig(randomBoolean() ? null : randomIntBetween(-1, 10));
    }

    public void testFromMap() {
        ClassificationConfig expected = new ClassificationConfig(0);
        assertThat(ClassificationConfig.fromMap(Collections.emptyMap()), equalTo(expected));

        expected = new ClassificationConfig(3);
        assertThat(ClassificationConfig.fromMap(Collections.singletonMap(ClassificationConfig.NUM_TOP_CLASSES.getPreferredName(), 3)),
            equalTo(expected));
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

}
