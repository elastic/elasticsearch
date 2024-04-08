/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class IndexLocationTests extends AbstractXContentSerializingTestCase<IndexLocation> {

    private final boolean lenient = randomBoolean();

    public static IndexLocation randomInstance() {
        return new IndexLocation(randomAlphaOfLength(7));
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
    protected IndexLocation mutateInstance(IndexLocation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }
}
