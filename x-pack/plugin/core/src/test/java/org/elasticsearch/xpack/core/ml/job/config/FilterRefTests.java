/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class FilterRefTests extends AbstractSerializingTestCase<FilterRef> {

    @Override
    protected FilterRef createTestInstance() {
        return new FilterRef(randomAlphaOfLength(20), randomFrom(FilterRef.FilterType.values()));
    }

    @Override
    protected FilterRef doParseInstance(XContentParser parser) throws IOException {
        return FilterRef.STRICT_PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<FilterRef> instanceReader() {
        return FilterRef::new;
    }
}
