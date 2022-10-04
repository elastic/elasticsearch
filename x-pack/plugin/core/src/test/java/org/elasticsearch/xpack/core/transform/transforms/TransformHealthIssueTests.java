/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TransformHealthIssueTests extends AbstractSerializingTestCase<TransformHealthIssue> {

    public static TransformHealthIssue randomTransformHealthIssue() {
        return new TransformHealthIssue(randomAlphaOfLengthBetween(10, 200), randomBoolean() ? randomAlphaOfLengthBetween(10, 200) : null);
    }

    @Override
    protected TransformHealthIssue doParseInstance(XContentParser parser) throws IOException {
        return TransformHealthIssue.fromXContent(parser);
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
    protected boolean supportsUnknownFields() {
        return true;
    }
}
