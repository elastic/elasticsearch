/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.time.Instant;

public class TransformHealthIssueTests extends AbstractWireSerializingTestCase<TransformHealthIssue> {

    public static TransformHealthIssue randomTransformHealthIssue() {
        return new TransformHealthIssue(
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
}
