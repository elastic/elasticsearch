/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class IndexLocationTests extends AbstractSerializingTestCase<IndexLocation> {

    private final boolean lenient = randomBoolean();

    public static IndexLocation randomInstance() {
        return new IndexLocation(randomAlphaOfLength(7), randomAlphaOfLength(7));
    }

    @Override
    protected IndexLocation doParseInstance(XContentParser parser) throws IOException {
        return lenient ? IndexLocation.fromXContentLenient(parser) : IndexLocation.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<IndexLocation> instanceReader() {
        return IndexLocation::new;
    }

    @Override
    protected IndexLocation createTestInstance() {
        return randomInstance();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }
}
