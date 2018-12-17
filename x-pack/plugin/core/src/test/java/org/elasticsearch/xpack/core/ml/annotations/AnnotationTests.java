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
            new Date(randomNonNegativeLong()),
            randomBoolean() ? randomAlphaOfLengthBetween(10, 30) : null,
            new Date(randomNonNegativeLong()),
            randomAlphaOfLengthBetween(5, 20),
            randomAlphaOfLengthBetween(10, 15));
    }

    @Override
    protected Writeable.Reader<Annotation> instanceReader() {
        return Annotation::new;
    }
}
