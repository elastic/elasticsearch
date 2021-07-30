/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class FilterRefTests extends AbstractXContentTestCase<FilterRef> {

    @Override
    protected FilterRef createTestInstance() {
        return new FilterRef(randomAlphaOfLength(20), randomFrom(FilterRef.FilterType.values()));
    }

    @Override
    protected FilterRef doParseInstance(XContentParser parser) throws IOException {
        return FilterRef.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
