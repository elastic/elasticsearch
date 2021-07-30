/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
        return Annotation.fromXContent(parser, null);
    }

    @Override
    protected Annotation createTestInstance() {
        return randomAnnotation(randomBoolean() ? randomAlphaOfLengthBetween(10, 30) : null);
    }

    public static Annotation randomAnnotation(String jobId) {
        return new Annotation.Builder()
            .setAnnotation(randomAlphaOfLengthBetween(100, 1000))
            .setCreateTime(new Date(randomNonNegativeLong()))
            .setCreateUsername(randomAlphaOfLengthBetween(5, 20))
            .setTimestamp(new Date(randomNonNegativeLong()))
            .setEndTimestamp(randomBoolean() ? new Date(randomNonNegativeLong()) : null)
            .setJobId(jobId)
            .setModifiedTime(randomBoolean() ? new Date(randomNonNegativeLong()) : null)
            .setModifiedUsername(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setType(randomFrom(Annotation.Type.values()))
            .setEvent(randomBoolean() ? randomFrom(Annotation.Event.values()) : null)
            .setDetectorIndex(randomBoolean() ? randomIntBetween(0, 10) : null)
            .setPartitionFieldName(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setPartitionFieldValue(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setOverFieldName(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setOverFieldValue(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setByFieldName(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .setByFieldValue(randomBoolean() ? randomAlphaOfLengthBetween(5, 20) : null)
            .build();
    }

    @Override
    protected Writeable.Reader<Annotation> instanceReader() {
        return Annotation::new;
    }

    public void testCopyConstructor() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            Annotation testAnnotation = createTestInstance();
            assertThat(testAnnotation, equalTo(new Annotation.Builder(testAnnotation).build()));
        }
    }
}
