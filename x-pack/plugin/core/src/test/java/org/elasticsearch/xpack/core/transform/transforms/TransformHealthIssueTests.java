/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformHealthIssueTests extends AbstractWireSerializingTestCase<TransformHealthIssue> {

    public static TransformHealthIssue randomTransformHealthIssue() {
        return new TransformHealthIssue(
            randomAlphaOfLengthBetween(10, 200),
            randomAlphaOfLengthBetween(10, 200),
            randomBoolean() ? randomAlphaOfLengthBetween(10, 200) : null,
            randomIntBetween(1, 10),
            randomBoolean() ? null : Instant.ofEpochSecond(randomLongBetween(1, 100000), randomLongBetween(-999_999_999, 999_999_999))
        );
    }

    @Override
    protected Writeable.Reader<TransformHealthIssue> instanceReader() {
        return TransformHealthIssue::new;
    }

    @Override
    protected TransformHealthIssue createTestInstance() {
        return randomTransformHealthIssue();
    }

    @Override
    protected TransformHealthIssue mutateInstance(TransformHealthIssue instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testMissingTypePre88() throws IOException {
        TransformHealthIssue originalIssue = new TransformHealthIssue("some-type", "some-issue", null, 1, null);
        assertThat(originalIssue.getType(), is(equalTo("some-type")));
        TransformHealthIssue deserializedIssue = copyInstance(
            originalIssue,
            getNamedWriteableRegistry(),
            (out, value) -> value.writeTo(out),
            in -> new TransformHealthIssue(in),
            TransportVersion.V_8_7_0
        );
        assertThat(deserializedIssue.getType(), is(equalTo("unknown")));
    }
}
