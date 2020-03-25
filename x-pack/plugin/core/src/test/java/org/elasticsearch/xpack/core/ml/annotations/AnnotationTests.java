/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class AnnotationTests extends AbstractSerializingTestCase<Annotation> {

    @Override
    protected Annotation doParseInstance(XContentParser parser) {
        return Annotation.PARSER.apply(parser, null);
    }

    @Override
    protected Annotation createTestInstance() {
        return new Annotation(randomAlphaOfLengthBetween(100, 1000),
            new Date(randomNonNegativeLong()),
            randomAlphaOfLengthBetween(5, 20),
            new Date(randomNonNegativeLong()),
            randomBoolean() ? new Date(randomNonNegativeLong()) : null,
            randomBoolean() ? randomAlphaOfLengthBetween(10, 30) : null,
            randomBoolean() ? new Date(randomNonNegativeLong()) : null,
            randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null,
            randomAlphaOfLengthBetween(10, 15));
    }

    @Override
    protected Writeable.Reader<Annotation> instanceReader() {
        return Annotation::new;
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            Annotation testAnnotation = createTestInstance();
            assertThat(testAnnotation, equalTo(new Annotation(testAnnotation)));
        }
    }
}
