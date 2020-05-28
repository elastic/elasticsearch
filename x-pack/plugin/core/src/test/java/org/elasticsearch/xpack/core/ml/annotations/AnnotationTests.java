/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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

    public void testParsingTypeFieldIsLenient() throws IOException {
        XContentBuilder json = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
            .field("annotation", "some text")
            .field("create_time", 2_000_000)
            .field("create_username", "some user")
            .field("timestamp", 1_000_000)
            .field("type", "bad_type")
            .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(json))) {
            Annotation annotation = doParseInstance(parser);
            assertThat(annotation.getType(), is(equalTo(Annotation.Type.ANNOTATION)));
        }
    }

    public void testParsingEventFieldIsLenient() throws IOException {
        XContentBuilder json = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
            .field("annotation", "some text")
            .field("create_time", 2_000_000)
            .field("create_username", "some user")
            .field("timestamp", 1_000_000)
            .field("type", "annotation")
            .field("event", "bad_event")
            .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(json))) {
            Annotation annotation = doParseInstance(parser);
            assertThat(annotation.getEvent(), is(nullValue()));
        }
    }
}
